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
package com.facebook.presto.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

public class TestHiveKerberosConfig
{
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.service.principal", "hive/_HOST@EXAMPLE.COM")
                .put("hive.metastore.client.principal", "metastore@EXAMPLE.COM")
                .put("hive.metastore.client.keytab", "/tmp/metastore.keytab")
                .put("hive.hdfs.presto.principal", "presto@EXAMPLE.COM")
                .put("hive.hdfs.presto.keytab", "/tmp/presto.keytab")
                .build();

        HiveKerberosConfig expected = new HiveKerberosConfig()
                .setHiveMetastoreServicePrincipal("hive/_HOST@EXAMPLE.COM")
                .setHiveMetastoreClientPrincipal("metastore@EXAMPLE.COM")
                .setHiveMetastoreClientKeytab("/tmp/metastore.keytab")
                .setHdfsPrestoPrincipal("presto@EXAMPLE.COM")
                .setHdfsPrestoKeytab("/tmp/presto.keytab");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
