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

import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.hive.orc.OrcReader;
import com.facebook.presto.hive.orc.OrcRecordReader;
import com.facebook.presto.hive.orc.metadata.MetadataReader;
import com.facebook.presto.hive.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveUtil.getDeserializer;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrcPageSourceFactory
        implements HivePageSourceFactory
{
    private final TypeManager typeManager;
    private final boolean enabled;

    @Inject
    public OrcPageSourceFactory(TypeManager typeManager, HiveClientConfig config)
    {
        //noinspection deprecation
        this(typeManager, config.isOrcOptimizedReaderEnabled());
    }

    public OrcPageSourceFactory(TypeManager typeManager)
    {
        this(typeManager, true);
    }

    public OrcPageSourceFactory(TypeManager typeManager, boolean enabled)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.enabled = enabled;
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> tupleDomain,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!enabled) {
            return Optional.absent();
        }

        @SuppressWarnings("deprecation")
        Deserializer deserializer = getDeserializer(schema);
        if (!(deserializer instanceof OrcSerde)) {
            return Optional.absent();
        }

        return Optional.of(creteOrcPageSource(
                new OrcMetadataReader(),
                configuration,
                session,
                path,
                start,
                length,
                columns,
                partitionKeys,
                tupleDomain,
                hiveStorageTimeZone,
                typeManager));
    }

    public static OrcPageSource creteOrcPageSource(MetadataReader metadataReader,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> tupleDomain,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        HdfsOrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = path.getFileSystem(configuration);
            orcDataSource = new HdfsOrcDataSource(path, fileSystem);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        try {
            OrcReader reader = new OrcReader(orcDataSource, metadataReader, typeManager);
            OrcRecordReader recordReader = reader.createRecordReader(
                    start,
                    length,
                    columns,
                    tupleDomain,
                    hiveStorageTimeZone,
                    DateTimeZone.forID(session.getTimeZoneKey().getId()));

            return new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    partitionKeys,
                    columns,
                    hiveStorageTimeZone,
                    typeManager);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            throw Throwables.propagate(e);
        }
    }
}
