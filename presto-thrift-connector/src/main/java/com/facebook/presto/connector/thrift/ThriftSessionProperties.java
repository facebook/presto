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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.builder;
import static java.util.Objects.requireNonNull;

/**
 * Internal session properties are those defined by the connector itself.
 * These properties control certain aspects of connector's work.
 */
public final class ThriftSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;
    private final Map<String, PropertyMetadata<?>> headerProperties;
    private static final Set<Class<?>> SUPPORTED_TYPES = ImmutableSet.of(Integer.class, Long.class, Double.class, Boolean.class, String.class);

    @Inject
    public ThriftSessionProperties(SessionPropertyProvider sessionPropertyProvider)
    {
        requireNonNull(sessionPropertyProvider, "sessionPropertyProvider is null");
        headerProperties = requireNonNull(sessionPropertyProvider.getHeaderProperties(), "sessionPropertyProvider returned null header properties");
        headerProperties.values().stream().forEach(property -> checkIfTypeSupported(property.getJavaType()));
        sessionProperties = ImmutableList.copyOf(headerProperties.values());
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public Map<String, String> toHeader(ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> header = builder();
        headerProperties.forEach((name, property) -> {
            Object value = session.getProperty(property.getName(), property.getJavaType());
            if (value != null && !value.equals(property.getDefaultValue())) {
                header.put(name, value.toString());
            }
        });
        return header.build();
    }

    private void checkIfTypeSupported(Class<?> javaType)
    {
        checkArgument(SUPPORTED_TYPES.contains(javaType), "Java type %s is not supported to be passed to thrift header", javaType);
    }
}
