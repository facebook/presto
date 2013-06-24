package com.facebook.presto.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RetryDriver
{
    private static final Logger log = Logger.get(RetryDriver.class);
    private static final int DEFAULT_RETRY_ATTEMPTS = 10;
    private static final Duration DEFAULT_SLEEP_TIME = Duration.valueOf("1s");
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.valueOf("30s");

    private final int maxRetryAttempts;
    private final Duration sleepTime;
    private final Duration maxRetryTime;
    private final List<Class<? extends Exception>> exceptionWhiteList;

    private RetryDriver(int maxRetryAttempts, Duration sleepTime, Duration maxRetryTime, List<Class<? extends Exception>> exceptionWhiteList)
    {
        this.maxRetryAttempts = maxRetryAttempts;
        this.sleepTime = sleepTime;
        this.maxRetryTime = maxRetryTime;
        this.exceptionWhiteList = exceptionWhiteList;

    }

    private RetryDriver()
    {
        this(DEFAULT_RETRY_ATTEMPTS, DEFAULT_SLEEP_TIME, DEFAULT_MAX_RETRY_TIME, ImmutableList.<Class<? extends Exception>>of());
    }

    public static RetryDriver retry()
    {
        return new RetryDriver();
    }

    public RetryDriver withMaxRetries(int maxRetryAttempts)
    {
        checkArgument(maxRetryAttempts > 0, "maxRetryAttempts must be greater than zero");
        return new RetryDriver(maxRetryAttempts, sleepTime, maxRetryTime, exceptionWhiteList);
    }

    public RetryDriver withSleep(Duration sleepTime)
    {
        return new RetryDriver(maxRetryAttempts, checkNotNull(sleepTime, "sleepTime is null"), maxRetryTime, exceptionWhiteList);
    }

    public RetryDriver withMaxRetryTime(Duration maxRetryTime)
    {
        return new RetryDriver(maxRetryAttempts, sleepTime, checkNotNull(maxRetryTime, "maxRetryTime is null"), exceptionWhiteList);
    }

    @SafeVarargs
    public final RetryDriver stopOn(Class<? extends Exception>... classes)
    {
        checkNotNull(classes, "classes is null");
        List<Class<? extends Exception>> exceptions = ImmutableList.<Class<? extends Exception>>builder()
                .addAll(exceptionWhiteList)
                .addAll(Arrays.asList(classes))
                .build();


        return new RetryDriver(maxRetryAttempts, sleepTime, maxRetryTime, exceptions);
    }

    public RetryDriver stopOnIllegalExceptions()
    {
        return stopOn(NullPointerException.class, IllegalStateException.class, IllegalArgumentException.class);
    }

    public <V> V run(String callableName, Callable<V> callable)
            throws Exception
    {
        checkNotNull(callableName, "callableName is null");
        checkNotNull(callable, "callable is null");

        long startTime = System.nanoTime();
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return callable.call();
            }
            catch (Exception e) {
                for (Class<? extends Exception> clazz : exceptionWhiteList) {
                    if (clazz.isInstance(e)) {
                        throw e;
                    }
                }
                if (attempt >= maxRetryAttempts || Duration.nanosSince(startTime).compareTo(maxRetryTime) >= 0) {
                    throw e;
                }
                else {
                    log.debug("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());
                }
                TimeUnit.MILLISECONDS.sleep((long) sleepTime.toMillis());
            }
        }
    }
}
