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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class HiveKerberosConfig
{
    private String hiveMetastoreServicePrincipal;
    private String hiveMetastoreClientPrincipal;
    private String hiveMetastoreClientKeytab;

    private String hdfsPrestoPrincipal;
    private String hdfsPrestoKeytab;

    @NotNull
    public String getHiveMetastoreServicePrincipal()
    {
        return hiveMetastoreServicePrincipal;
    }

    @Config("hive.metastore.service.principal")
    @ConfigDescription("Hive Metastore service principal")
    public HiveKerberosConfig setHiveMetastoreServicePrincipal(String hiveMetastoreServicePrincipal)
    {
        this.hiveMetastoreServicePrincipal = hiveMetastoreServicePrincipal;
        return this;
    }

    @NotNull
    public String getHiveMetastoreClientPrincipal()
    {
        return hiveMetastoreClientPrincipal;
    }

    @Config("hive.metastore.client.principal")
    @ConfigDescription("Hive Metastore client principal")
    public HiveKerberosConfig setHiveMetastoreClientPrincipal(String hiveMetastoreClientPrincipal)
    {
        this.hiveMetastoreClientPrincipal = hiveMetastoreClientPrincipal;
        return this;
    }

    @NotNull
    public String getHiveMetastoreClientKeytab()
    {
        return hiveMetastoreClientKeytab;
    }

    @Config("hive.metastore.client.keytab")
    @ConfigDescription("Hive Metastore client keytab location")
    public HiveKerberosConfig setHiveMetastoreClientKeytab(String hiveMetastoreClientKeytab)
    {
        this.hiveMetastoreClientKeytab = hiveMetastoreClientKeytab;
        return this;
    }

    @NotNull
    public String getHdfsPrestoPrincipal()
    {
        return hdfsPrestoPrincipal;
    }

    @Config("hive.hdfs.presto.principal")
    @ConfigDescription("Presto principal used to access HDFS")
    public HiveKerberosConfig setHdfsPrestoPrincipal(String hdfsPrestoPrincipal)
    {
        this.hdfsPrestoPrincipal = hdfsPrestoPrincipal;
        return this;
    }

    @NotNull
    public String getHdfsPrestoKeytab()
    {
        return hdfsPrestoKeytab;
    }

    @Config("hive.hdfs.presto.keytab")
    @ConfigDescription("Presto keytab used to access HDFS")
    public HiveKerberosConfig setHdfsPrestoKeytab(String hdfsPrestoKeytab)
    {
        this.hdfsPrestoKeytab = hdfsPrestoKeytab;
        return this;
    }
}
