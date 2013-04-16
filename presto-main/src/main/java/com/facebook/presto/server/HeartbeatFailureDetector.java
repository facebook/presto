package com.facebook.presto.server;

import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.Threads;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.stats.DecayCounter;
import io.airlift.stats.ExponentialDecay;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static io.airlift.http.client.Request.Builder.prepareHead;

public class HeartbeatFailureDetector
        implements FailureDetector
{
    private static final Logger log = Logger.get(HeartbeatFailureDetector.class);

    private final ServiceSelector selector;
    private final AsyncHttpClient httpClient;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(Threads.daemonThreadsNamed("failure-detector"));

    // monitoring tasks by service id
    private final ConcurrentMap<UUID, MonitoringTask> tasks = new ConcurrentHashMap<>();

    private final double failureRatioThreshold;
    private final Duration hearbeatInterval;
    private final boolean isEnabled;
    private final Duration warmupInterval;
    private final Duration gcGraceInterval;

    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public HeartbeatFailureDetector(@ServiceType("presto") ServiceSelector selector,
            @ForFailureDetector AsyncHttpClient httpClient,
            FailureDetectorConfiguration config,
            QueryManagerConfig queryManagerConfig)
    {
        checkNotNull(selector, "selector is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(config, "config is null");
        checkArgument((long) config.getHearbeatInterval().toMillis() >= 1, "heartbeat interval must be >= 1ms");

        this.selector = selector;
        this.httpClient = httpClient;

        this.failureRatioThreshold = config.getFailureRatioThreshold();
        this.hearbeatInterval = config.getHearbeatInterval();
        this.warmupInterval = config.getWarmupInterval();
        this.gcGraceInterval = config.getGcGraceInterval();

        this.isEnabled = config.isEnabled() && queryManagerConfig.isCoordinator();
    }

    @PostConstruct
    public void start()
    {
        if (isEnabled && started.compareAndSet(false, true)) {
            executor.scheduleWithFixedDelay(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        updateMonitoredServices();
                    }
                    catch (Throwable e) {
                        // ignore to avoid getting unscheduled
                        log.warn(e, "Error updating services");
                    }
                }
            }, 0, 5, TimeUnit.SECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Override
    public Set<ServiceDescriptor> getFailed()
    {
        return IterableTransformer.on(tasks.values())
                .select(isFailedPredicate())
                .transform(serviceGetter())
                .set();
    }

    @Managed(description = "Number of failed services")
    public int getFailedCount()
    {
        return getFailed().size();
    }

    @Managed(description = "Total number of known services")
    public int getTotalCount()
    {
        return tasks.size();
    }

    public Map<ServiceDescriptor, Stats> getStats()
    {
        ImmutableMap.Builder<ServiceDescriptor, Stats> builder = ImmutableMap.builder();
        for (MonitoringTask task : tasks.values()) {
            builder.put(task.getService(), task.getStats());
        }
        return builder.build();
    }

    private void updateMonitoredServices()
    {
        Set<ServiceDescriptor> online = ImmutableSet.copyOf(selector.selectAllServices());

        Set<UUID> onlineIds = IterableTransformer.on(online)
                .transform(idGetter())
                .set();

        synchronized (tasks) { // make sure only one thread is updating the registrations
            // 1. remove expired tasks
            List<UUID> expiredIds = IterableTransformer.on(tasks.values())
                    .select(isExpiredPredicate())
                    .transform(serviceIdGetter())
                    .list();

            tasks.keySet().removeAll(expiredIds);

            // 2. disable offline services
            Iterable<MonitoringTask> toDisable = IterableTransformer.on(tasks.values())
                    .select(compose(not(in(onlineIds)), serviceIdGetter()))
                    .all();

            for (MonitoringTask task : toDisable) {
                task.disable();
            }

            // 3. create tasks for new services
            Set<ServiceDescriptor> newServices = IterableTransformer.on(online)
                    .select(compose(not(in(tasks.keySet())), idGetter()))
                    .set();

            for (final ServiceDescriptor service : newServices) {
                final URI uri = getHttpUri(service);

                if (uri != null) {
                    tasks.put(service.getId(), new MonitoringTask(executor, service, uri));
                }
            }

            // 4. enable all online tasks (existing plus newly created)
            Iterable<MonitoringTask> toEnable = IterableTransformer.on(tasks.values())
                    .select(compose(in(onlineIds), serviceIdGetter()))
                    .all();

            for (MonitoringTask task : toEnable) {
                task.enable();
            }
        }
    }

    private URI getHttpUri(ServiceDescriptor service)
    {
        try {
            String uri = service.getProperties().get("http");
            if (uri != null) {
                return new URI(uri);
            }
        }
        catch (URISyntaxException e) {
            // ignore, not a valid http uri
        }

        return null;
    }

    private static Function<ServiceDescriptor, UUID> idGetter()
    {
        return new Function<ServiceDescriptor, UUID>()
        {
            @Override
            public UUID apply(ServiceDescriptor descriptor)
            {
                return descriptor.getId();
            }
        };
    }

    private static Function<MonitoringTask, ServiceDescriptor> serviceGetter()
    {
        return new Function<MonitoringTask, ServiceDescriptor>()
        {
            @Override
            public ServiceDescriptor apply(MonitoringTask task)
            {
                return task.getService();
            }
        };
    }
    private static Function<MonitoringTask, UUID> serviceIdGetter()
    {
        return new Function<MonitoringTask, UUID>()
        {
            @Override
            public UUID apply(MonitoringTask task)
            {
                return task.getService().getId();
            }
        };
    }

    private static Predicate<MonitoringTask> isExpiredPredicate()
    {
        return new Predicate<MonitoringTask>()
        {
            @Override
            public boolean apply(MonitoringTask task)
            {
                return task.isExpired();
            }
        };
    }

    private static Predicate<MonitoringTask> isFailedPredicate()
    {
        return new Predicate<MonitoringTask>()
        {
            @Override
            public boolean apply(MonitoringTask task)
            {
                return task.isFailed();
            }
        };
    }

    private class MonitoringTask
    {
        private final ServiceDescriptor service;
        private final URI uri;
        private final Stats stats;
        private final ScheduledExecutorService executor;

        @GuardedBy("this")
        private ScheduledFuture<?> future;

        @GuardedBy("this")
        private Long disabledTimestamp;

        @GuardedBy("this")
        private Long successTransitionTimestamp;


        private MonitoringTask(ScheduledExecutorService executor, ServiceDescriptor service, URI uri)
        {
            this.uri = uri;
            this.executor = executor;
            this.service = service;
            this.stats = new Stats(uri);
        }

        public Stats getStats()
        {
            return stats;
        }

        public ServiceDescriptor getService()
        {
            return service;
        }

        public synchronized void enable()
        {
            if (future == null) {
                future = executor.scheduleAtFixedRate(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try {
                            ping();
                            updateState();
                        }
                        catch (Throwable e) {
                            // ignore to avoid getting unscheduled
                            log.warn(e, "Error pinging service %s (%s)", service.getId(), uri);
                        }
                    }
                }, (long) hearbeatInterval.toMillis(), (long) hearbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
                disabledTimestamp = null;
            }
        }

        public synchronized void disable()
        {
            if (future != null) {
                future.cancel(true);
                future = null;
                disabledTimestamp = System.nanoTime();
            }
        }

        public synchronized boolean isExpired()
        {
            return future == null && disabledTimestamp != null && Duration.nanosSince(disabledTimestamp).compareTo(gcGraceInterval) > 0;
        }

        public synchronized boolean isFailed()
        {
            return future == null || // are we disabled?
                    successTransitionTimestamp == null || // are we in success state?
                    Duration.nanosSince(successTransitionTimestamp).compareTo(warmupInterval) < 0; // are we within the warmup period?
        }

        private void ping()
        {
            try {
                stats.recordStart();
                httpClient.executeAsync(prepareHead().setUri(uri).build(), new ResponseHandler<Object, Exception>()
                {
                    @Override
                    public Exception handleException(Request request, Exception exception)
                    {
                        // ignore error
                        stats.recordFailure(exception);

                        // TODO: this will technically cause an NPE in httpClient, but it's not triggered because
                        // we never call get() on the response future. This behavior needs to be fixed in airlift
                        return null;
                    }

                    @Override
                    public Object handle(Request request, Response response)
                            throws Exception
                    {
                        stats.recordSuccess();
                        return null;
                    }
                });
            }
            catch (Exception e) {
                log.warn(e, "Error scheduling request for %s", uri);
            }
        }

        private synchronized void updateState()
        {
            // is this an over/under transition?
            if (stats.getRecentFailureRatio() > failureRatioThreshold) {
                successTransitionTimestamp = null;
            }
            else if (successTransitionTimestamp == null) {
                successTransitionTimestamp = System.nanoTime();
            }
        }
    }

    public static class Stats
    {
        private final long start = System.nanoTime();
        private final URI uri;

        private final DecayCounter recentRequests = new DecayCounter(ExponentialDecay.oneMinute());
        private final DecayCounter recentFailures = new DecayCounter(ExponentialDecay.oneMinute());
        private final DecayCounter recentSuccesses = new DecayCounter(ExponentialDecay.oneMinute());
        private final AtomicReference<DateTime> lastRequestTime = new AtomicReference<>();
        private final AtomicReference<DateTime> lastResponseTime = new AtomicReference<>();

        @GuardedBy("this")
        private final Map<Class<? extends Throwable>, DecayCounter> failureCountByType = new HashMap<>();

        public Stats(URI uri)
        {
            this.uri = uri;
        }

        public void recordStart()
        {
            recentRequests.add(1);
            lastRequestTime.set(new DateTime());
        }

        public void recordSuccess()
        {
            recentSuccesses.add(1);
            lastResponseTime.set(new DateTime());
        }

        public void recordFailure(Exception exception)
        {
            recentFailures.add(1);
            lastResponseTime.set(new DateTime());

            Throwable cause = exception;
            while (cause.getClass() == RuntimeException.class && cause.getCause() != null) {
                cause = cause.getCause();
            }

            synchronized (this) {
                DecayCounter counter = failureCountByType.get(cause.getClass());
                if (counter == null) {
                    counter = new DecayCounter(ExponentialDecay.oneMinute());
                    failureCountByType.put(cause.getClass(), counter);
                }
                counter.add(1);
            }
        }

        @JsonProperty
        public Duration getAge()
        {
            return Duration.nanosSince(start);
        }

        @JsonProperty
        public URI getUri()
        {
            return uri;
        }

        @JsonProperty
        public double getRecentFailures()
        {
            return recentFailures.getCount();
        }

        @JsonProperty
        public double getRecentSuccesses()
        {
            return recentSuccesses.getCount();
        }

        @JsonProperty
        public double getRecentRequests()
        {
            return recentRequests.getCount();
        }

        @JsonProperty
        public double getRecentFailureRatio()
        {
            return recentFailures.getCount() / recentRequests.getCount();
        }

        @JsonProperty
        public DateTime getLastRequestTime()
        {
            return lastRequestTime.get();
        }

        @JsonProperty
        public DateTime getLastResponseTime()
        {
            return lastResponseTime.get();
        }

        @JsonProperty
        public synchronized Map<String, Double> getRecentFailuresByType()
        {
            ImmutableMap.Builder<String, Double> builder = ImmutableMap.builder();
            for (Map.Entry<Class<? extends Throwable>, DecayCounter> entry : failureCountByType.entrySet()) {
                builder.put(entry.getKey().getName(), entry.getValue().getCount());
            }
            return builder.build();
        }

    }
}
