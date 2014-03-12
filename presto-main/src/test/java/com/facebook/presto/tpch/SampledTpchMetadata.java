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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableHandle;

public class SampledTpchMetadata
        extends TpchMetadata
{
    public static final String SAMPLE_WEIGHT_COLUMN_NAME = "$sampleWeight";
    public static final int SAMPLE_WEIGHT_COLUMN_INDEX = 999;

    public SampledTpchMetadata(String connectorId)
    {
        super(connectorId);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return new TpchColumnHandle(SAMPLE_WEIGHT_COLUMN_NAME, SAMPLE_WEIGHT_COLUMN_INDEX, ColumnType.LONG);
    }
}
