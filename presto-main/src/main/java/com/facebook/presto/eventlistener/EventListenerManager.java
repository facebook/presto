/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.eventlistener;

import com.facebook.presto.MBeanNamespaceManager;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerContext;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.management.MBeanServer;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class EventListenerManager
{
    private static final Logger log = Logger.get(EventListenerManager.class);
    private static final File EVENT_LISTENER_CONFIGURATION = new File("etc/event-listener.properties");
    private static final String EVENT_LISTENER_PROPERTY_NAME = "event-listener.name";

    private final Map<String, EventListenerFactory> eventListenerFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<EventListener>> configuredEventListener = new AtomicReference<>(Optional.empty());
    private final MBeanNamespaceManager namespaceManager;

    @Inject
    public EventListenerManager(MBeanNamespaceManager namespaceManager)
    {
        this.namespaceManager = namespaceManager;
    }

    public void addEventListenerFactory(EventListenerFactory eventListenerFactory)
    {
        requireNonNull(eventListenerFactory, "eventListenerFactory is null");

        if (eventListenerFactories.putIfAbsent(eventListenerFactory.getName(), eventListenerFactory) != null) {
            throw new IllegalArgumentException(format("Event listener '%s' is already registered", eventListenerFactory.getName()));
        }
    }

    public void loadConfiguredEventListener()
            throws Exception
    {
        if (EVENT_LISTENER_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(EVENT_LISTENER_CONFIGURATION));

            String eventListenerName = properties.remove(EVENT_LISTENER_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(eventListenerName),
                    "Access control configuration %s does not contain %s", EVENT_LISTENER_CONFIGURATION.getAbsoluteFile(), EVENT_LISTENER_PROPERTY_NAME);

            setConfiguredEventListener(eventListenerName, properties);
        }
    }

    @VisibleForTesting
    protected void setConfiguredEventListener(String name, Map<String, String> properties)
            throws Exception
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading event listener --");

        EventListenerFactory eventListenerFactory = eventListenerFactories.get(name);
        checkState(eventListenerFactory != null, "Event listener %s is not registered", name);

        EventListener eventListener = eventListenerFactory.create(ImmutableMap.copyOf(properties),
                new EventListenerContext()
                {
                    // To avoid conflict with connector with same catalog name.
                    private final MBeanServer mBeanServer = namespaceManager.createMBeanServer(name + "EventListener");
                    @Override
                    public MBeanServer getMBeanServer()
                    {
                        return mBeanServer;
                    }
                });
        this.configuredEventListener.set(Optional.of(eventListener));

        log.info("-- Loaded event listener %s --", name);
    }

    protected MBeanNamespaceManager getMBeanNamespaceManager()
    {
        return namespaceManager;
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().queryCompleted(queryCompletedEvent);
        }
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().queryCreated(queryCreatedEvent);
        }
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().splitCompleted(splitCompletedEvent);
        }
    }

    private static Map<String, String> loadProperties(File file)
            throws Exception
    {
        requireNonNull(file, "file is null");

        Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }
}
