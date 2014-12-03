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
package com.facebook.presto.raptor.storage;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class OutputHandle
{
    private final RowSinkProvider rowSinkProvider;

    public OutputHandle(RowSinkProvider rowSinkProvider)
    {
        this.rowSinkProvider = checkNotNull(rowSinkProvider, "rowSinkProvider is null");
    }

    public List<UUID> getShardUuids()
    {
        return rowSinkProvider.getShardUuids();
    }

    public RowSinkProvider getRowSinkProvider()
    {
        return rowSinkProvider;
    }
}
