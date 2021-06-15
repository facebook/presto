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
package com.facebook.presto.spi.resourceGroups;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class ResourceGroupQueryLimits
{
    private final Optional<Duration> perQueryExecutionTimeLimit;
    private final Optional<DataSize> perQueryTotalMemoryLimit;
    private final Optional<Duration> perQueryCpuTimeLimit;

    public ResourceGroupQueryLimits(
            Optional<Duration> perQueryExecutionTimeLimit,
            Optional<DataSize> perQueryTotalMemoryLimit,
            Optional<Duration> perQueryCpuTimeLimit)
    {
        this.perQueryExecutionTimeLimit = requireNonNull(perQueryExecutionTimeLimit, "perQueryExecutionTimeLimit is null");
        this.perQueryTotalMemoryLimit = requireNonNull(perQueryTotalMemoryLimit, "perQueryTotalMemoryLimit is null");
        this.perQueryCpuTimeLimit = requireNonNull(perQueryCpuTimeLimit, "perQueryCpuTime limit is null");
    }

    public Optional<Duration> getPerQueryExecutionTimeLimit()
    {
        return perQueryExecutionTimeLimit;
    }

    public Optional<DataSize> getPerQueryTotalMemoryLimit()
    {
        return perQueryTotalMemoryLimit;
    }

    public Optional<Duration> getPerQueryCpuTimeLimit()
    {
        return perQueryCpuTimeLimit;
    }
}
