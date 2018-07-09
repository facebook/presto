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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.conf.ElasticsearchConfig;
import com.facebook.presto.elasticsearch.conf.ElasticsearchSessionProperties;
import com.facebook.presto.elasticsearch.io.ElasticsearchPageSinkProvider;
import com.facebook.presto.elasticsearch.io.ElasticsearchPageSourceProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ElasticsearchModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ElasticsearchConfig.class);

        binder.bind(EsTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSinkProvider.class).in(Scopes.SINGLETON);

//        binder.bind(ElasticsearchTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSessionProperties.class).in(Scopes.SINGLETON);
    }
}
