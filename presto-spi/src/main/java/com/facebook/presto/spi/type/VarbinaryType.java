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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding.VariableWidthBlockEncodingFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class VarbinaryType
        implements VariableWidthType
{
    public static final VarbinaryType VARBINARY = new VarbinaryType();

    public static final BlockEncodingFactory<?> BLOCK_ENCODING_FACTORY = new VariableWidthBlockEncodingFactory(VARBINARY);

    @JsonCreator
    public VarbinaryType()
    {
    }

    public static VarbinaryType getInstance()
    {
        return VARBINARY;
    }

    @Override
    public String getName()
    {
        return "varbinary";
    }

    @Override
    public Class<?> getJavaType()
    {
        return Slice.class;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Slice slice, int offset)
    {
        return slice.slice(offset + SIZE_OF_INT, getValueSize(slice, offset));
    }

    @Override
    public int getLength(Slice slice, int offset)
    {
        return getValueSize(slice, offset) + SIZE_OF_INT;
    }

    private int getValueSize(Slice slice, int offset)
    {
        return slice.getInt(offset);
    }

    @Override
    public Slice getSlice(Slice slice, int offset)
    {
        return slice.slice(offset + SIZE_OF_INT, getValueSize(slice, offset));
    }

    @Override
    public int writeSlice(SliceOutput sliceOutput, Slice value, int offset, int length)
    {
        sliceOutput.writeInt(length);
        sliceOutput.writeBytes(value, offset, length);
        return length + SIZE_OF_INT;
    }

    @Override
    public boolean equalTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        int leftLength = getValueSize(leftSlice, leftOffset);
        int rightLength = getValueSize(rightSlice, rightOffset);
        return leftSlice.equals(leftOffset + SIZE_OF_INT, leftLength, rightSlice, rightOffset + SIZE_OF_INT, rightLength);
    }

    @Override
    public boolean equalTo(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        int leftLength = getValueSize(leftSlice, leftOffset);
        Slice rightSlice = rightCursor.getSlice();
        return leftSlice.equals(leftOffset + SIZE_OF_INT, leftLength, rightSlice, 0, rightSlice.length());
    }

    @Override
    public int hash(Slice slice, int offset)
    {
        int length = getValueSize(slice, offset);
        return slice.hashCode(offset + SIZE_OF_INT, length);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new VariableWidthBlockBuilder(this, blockBuilderStatus);
    }

    @Override
    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        int leftLength = getValueSize(leftSlice, leftOffset);
        int rightLength = getValueSize(rightSlice, rightOffset);
        return leftSlice.compareTo(leftOffset + SIZE_OF_INT, leftLength, rightSlice, rightOffset + SIZE_OF_INT, rightLength);
    }

    @Override
    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        int length = getValueSize(slice, offset);
        blockBuilder.appendSlice(slice, offset + SIZE_OF_INT, length);
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        // copy full value including length
        int length = getLength(slice, offset);
        sliceOutput.writeBytes(slice, offset, length);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
