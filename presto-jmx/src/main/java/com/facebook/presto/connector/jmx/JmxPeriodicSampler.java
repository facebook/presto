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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.management.JMException;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JmxPeriodicSampler
        implements Runnable
{
    private static final Logger log = Logger.get(JmxPeriodicSampler.class);

    private final JmxHistoricalData jmxHistoricalData;
    private final JmxRecordSetProvider jmxRecordSetProvider;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final long period;
    private final List<JmxTableHandle> tableHandles;

    @Inject
    public JmxPeriodicSampler(
            JmxHistoricalData jmxHistoricalData,
            JmxMetadata jmxMetadata,
            JmxRecordSetProvider jmxRecordSetProvider,
            JmxConnectorConfig jmxConfig)
    {
        this.jmxHistoricalData = requireNonNull(jmxHistoricalData, "jmxStatsHolder is null");
        requireNonNull(jmxMetadata, "jmxMetadata is null");
        this.jmxRecordSetProvider = requireNonNull(jmxRecordSetProvider, "jmxRecordSetProvider is null");
        requireNonNull(jmxConfig, "jmxConfig is null");
        this.period = jmxConfig.getDumpPeriod().toMillis();

        ImmutableList.Builder<JmxTableHandle> tableHandleBuilder = ImmutableList.builder();

        for (String tableName : jmxHistoricalData.getTables()) {
            tableHandleBuilder.add(requireNonNull(
                    jmxMetadata.getTableHandle(new SchemaTableName(JmxMetadata.HISTORY_SCHEMA_NAME, tableName)),
                    format("tableHandle is null for table [%s]", tableName)));
        }

        tableHandles = tableHandleBuilder.build();
    }

    @PostConstruct
    public void start()
    {
        lastDumpTimestamp = roundToPeriod(currentTimeMillis());
        schedule();
    }

    private void schedule()
    {
        long nextDumpTimestamp = lastDumpTimestamp + period;
        long nowMillis = currentTimeMillis();
        long delay = nextDumpTimestamp - nowMillis;
        executor.schedule(this, Math.max(delay, 0), MILLISECONDS);
    }

    private long roundToPeriod(long millis)
    {
        return ((millis + period / 2) / period) * period;
    }

    @Override
    public void run()
    {
        // we are using rounded up timestamp, so that records from different nodes and different
        // tables will have matching timestamps (for joining/grouping etc)
        long dumpTimestamp = roundToPeriod(currentTimeMillis());

        for (JmxTableHandle tableHandle : tableHandles) {
            try {
                List<Object> row = jmxRecordSetProvider.getLiveRow(
                        tableHandle,
                        tableHandle.getColumnHandles(),
                        dumpTimestamp);
                jmxHistoricalData.addRow(tableHandle.getObjectName(), row);
            }
            catch (JMException ex) {
                log.error(ex, "Error in JmxHistoryDumper thread");
            }
        }

        schedule();
    }

    public void shutdown()
    {
        executor.shutdown();
    }
}
