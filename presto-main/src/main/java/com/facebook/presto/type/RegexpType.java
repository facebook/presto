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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.regex.Pattern;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class RegexpType
    implements Type
{
    public static final RegexpType REGEXP = new RegexpType();

    public static RegexpType getInstance()
    {
        return REGEXP;
    }

    @Override
    public String getName()
    {
        return "RegExp";
    }

    @Override
    public boolean isComparable()
    {
        return false;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public boolean canCoerceFrom(Type type)
    {
        if (type.equals(VARCHAR)) {
            return true;
        }

        return false;
    }

    @Override
    public Class<?> getJavaType()
    {
        return Pattern.class;
    }

    @Override
    public Type getCommonSuperType(Type type)
    {
        return null;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hash(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        throw new PrestoException(StandardErrorCode.INTERNAL.toErrorCode(), "RegExp type cannot be serialized");
    }
}
