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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestCatalogManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CatalogManagerConfig.class)
                .setCatalogConfigurationDir(new File("etc/catalog"))
                .setAutoDetectCatalog(false)
                .setDisabledCatalogs((String) null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("catalog.config-dir", "/foo")
                .put("catalog.disabled-catalogs", "abc,xyz")
                .put("catalog.auto-detect", "true")
                .build();

        CatalogManagerConfig expected = new CatalogManagerConfig()
                .setCatalogConfigurationDir(new File("/foo"))
                .setDisabledCatalogs(ImmutableList.of("abc", "xyz"))
                .setAutoDetectCatalog(true);

        assertFullMapping(properties, expected);
    }
}
