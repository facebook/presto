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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.spi.block.BlockBuilderStatus;
import parquet.column.ColumnDescriptor;
import parquet.column.values.ValuesReader;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class ParquetIntBuilder
    extends ParquetBlockBuilder
{
    public ParquetIntBuilder(int size, ColumnDescriptor descriptor)
    {
        super(size, BIGINT, descriptor, BIGINT.createBlockBuilder(new BlockBuilderStatus(), size));
    }

    @Override
    public void readValue(ValuesReader valuesReader)
    {
        BIGINT.writeLong(blockBuilder, valuesReader.readInteger());
    }
}
