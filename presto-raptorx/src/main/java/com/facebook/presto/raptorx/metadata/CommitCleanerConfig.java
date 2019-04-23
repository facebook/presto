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
package com.facebook.presto.raptorx.metadata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class CommitCleanerConfig
{
    private Duration interval = new Duration(5, MINUTES);
    private int chunkBatchSize = 10000;

    @NotNull
    @MinDuration("1s")
    public Duration getInterval()
    {
        return interval;
    }

    @Config("metadata.commit-cleaner-interval")
    @ConfigDescription("How often to cleanup data for old commits")
    public CommitCleanerConfig setInterval(Duration interval)
    {
        this.interval = interval;
        return this;
    }

    public int getChunkBatchSize()
    {
        return chunkBatchSize;
    }

    @Config("metadata.commit-cleaner-chunk-batch")
    @ConfigDescription("How large to cleanup chunk")
    public CommitCleanerConfig setChunkBatchSize(int size)
    {
        this.chunkBatchSize = size;
        return this;
    }
}
