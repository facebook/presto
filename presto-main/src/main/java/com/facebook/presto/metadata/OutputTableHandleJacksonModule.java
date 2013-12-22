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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.OutputTableHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class OutputTableHandleJacksonModule
        extends AbstractTypedJacksonModule<OutputTableHandle>
{
    @Inject
    public OutputTableHandleJacksonModule(OutputTableHandleResolver handleResolver)
    {
        super(OutputTableHandle.class, "type", new OutputTableHandleJsonTypeIdResolver(handleResolver));
    }

    private static class OutputTableHandleJsonTypeIdResolver
            implements JsonTypeIdResolver<OutputTableHandle>
    {
        private final OutputTableHandleResolver handleResolver;

        private OutputTableHandleJsonTypeIdResolver(OutputTableHandleResolver handleResolver)
        {
            this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        }

        @Override
        public String getId(OutputTableHandle tableHandle)
        {
            return handleResolver.getId(tableHandle);
        }

        @Override
        public Class<? extends OutputTableHandle> getType(String id)
        {
            return handleResolver.getOutputTableHandleClass(id);
        }
    }
}
