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

public class WorkerTransactionCleanerConfig
{
    private Duration interval = new Duration(5, MINUTES);

    @NotNull
    @MinDuration("1s")
    public Duration getInterval()
    {
        return interval;
    }

    @Config("metadata.worker-transaction-cleaner-interval")
    @ConfigDescription("How often to cleanup for old worker transactions")
    public WorkerTransactionCleanerConfig setInterval(Duration interval)
    {
        this.interval = interval;
        return this;
    }
}
