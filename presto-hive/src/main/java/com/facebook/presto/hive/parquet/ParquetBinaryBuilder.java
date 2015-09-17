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
import io.airlift.slice.Slices;
import parquet.column.ColumnDescriptor;
import parquet.column.values.ValuesReader;
import parquet.io.api.Binary;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class ParquetBinaryBuilder
    extends ParquetBlockBuilder
{
    public ParquetBinaryBuilder(int size, ColumnDescriptor descriptor)
    {
        super(size, VARCHAR, descriptor, VARCHAR.createBlockBuilder(new BlockBuilderStatus(), size));
    }

    @Override
    public void readValue(ValuesReader valuesReader)
    {
        Binary binary = valuesReader.readBytes();
        if (binary.length() == 0) {
            blockBuilder.appendNull();
        }
        else {
            VARCHAR.writeSlice(blockBuilder, Slices.wrappedBuffer(binary.getBytes()));
        }
    }
}
