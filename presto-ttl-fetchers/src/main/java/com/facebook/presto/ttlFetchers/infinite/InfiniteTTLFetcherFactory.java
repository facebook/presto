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
package com.facebook.presto.ttlFetchers.infinite;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.spi.ttl.TTLFetcher;
import com.facebook.presto.spi.ttl.TTLFetcherFactory;
import com.google.inject.Injector;
import com.google.inject.Scopes;

import java.util.Map;

public class InfiniteTTLFetcherFactory
        implements TTLFetcherFactory
{
    @Override
    public String getName()
    {
        return "infinite";
    }

    @Override
    public TTLFetcher create(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(binder -> binder.bind(TTLFetcher.class).to(InfiniteTTLFetcher.class).in(Scopes.SINGLETON));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(TTLFetcher.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
