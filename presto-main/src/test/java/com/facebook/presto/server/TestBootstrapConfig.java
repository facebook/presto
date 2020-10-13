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
package com.facebook.presto.server;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestBootstrapConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BootstrapConfig.class)
                .setQuiet(false)
                .setStrictConfig(true)
                .setRequireExplicitBindings(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("bootstrap.quiet", "false")
                .put("bootstrap.strict-config", "false")
                .put("bootstrap.require-explicit-bindings", "true")
                .build();

        BootstrapConfig expected = new BootstrapConfig()
                .setQuiet(false)
                .setStrictConfig(false)
                .setRequireExplicitBindings(true);

        assertFullMapping(properties, expected);
    }
}
