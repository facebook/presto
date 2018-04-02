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

import com.facebook.presto.connector.thrift.tracetoken.ThriftTraceToken;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Internal session properties are those defined by the connector itself.
 * These properties control certain aspects of connector's work.
 */
public final class ThriftSessionProperties
{
    private static final String TRACE_TOKEN = "trace_token";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ThriftSessionProperties(ThriftConnectorConfig config)
    {
        sessionProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        TRACE_TOKEN,
                        "Trace token value",
                        VARCHAR,
                        ThriftTraceToken.class,
                        null,
                        false,
                        object -> ThriftTraceToken.valueOf((String) object),
                        traceToken -> traceToken == null ? null : traceToken.getValue()));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Optional<ThriftTraceToken> getTraceToken(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(TRACE_TOKEN, ThriftTraceToken.class));
    }
}
