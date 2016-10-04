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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VariableWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public abstract class AbstractVariableWidthRandomAccessBlock
        implements RandomAccessBlock
{
    protected final VariableWidthType type;

    protected AbstractVariableWidthRandomAccessBlock(VariableWidthType type)
    {
        this.type = type;
    }

    protected abstract Slice getRawSlice();

    protected abstract int getPositionOffset(int position);

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public int getSizeInBytes()
    {
        return getRawSlice().length();
    }

    @Override
    public BlockCursor cursor()
    {
        return new VariableWidthBlockCursor(type, getPositionCount(), getRawSlice());
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new VariableWidthBlockEncoding(type);
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }
        // todo add VariableWidthRandomAccessCursor
        return cursor().getRegionAndAdvance(length).toRandomAccessBlock();
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return this;
    }

    @Override
    public boolean getBoolean(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObjectValue(Session session, int position)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return null;
        }
        int offset = getPositionOffset(position);
        return type.getObjectValue(session, getRawSlice(), valueOffset(offset));
    }

    @Override
    public Slice getSlice(int position)
    {
        if (isNull(position)) {
            throw new IllegalStateException("position is null");
        }
        int offset = getPositionOffset(position);
        return type.getSlice(getRawSlice(), valueOffset(offset));
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int offset = getPositionOffset(position);
        if (isEntryAtOffsetNull(offset)) {
            return new VariableWidthRandomAccessBlock(type, 1, Slices.wrappedBuffer(new byte[] {1}));
        }

        int entrySize = valueOffset(type.getLength(getRawSlice(), valueOffset(offset)));

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, getRawSlice(), offset, entrySize);

        return new VariableWidthRandomAccessBlock(type, 1, copy);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        return isEntryAtOffsetNull(offset);
    }

    @Override
    public boolean equals(int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        int leftOffset = getPositionOffset(position);

        boolean leftIsNull = isEntryAtOffsetNull(leftOffset);
        boolean rightIsNull = right.isNull(rightPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return right.equals(rightPosition, getRawSlice(), valueOffset(leftOffset));
    }

    @Override
    public boolean equals(int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        boolean thisIsNull = isEntryAtOffsetNull(offset);
        boolean valueIsNull = cursor.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }

        VariableWidthBlockCursor variableWidthBlockCursor = (VariableWidthBlockCursor) cursor;
        return variableWidthBlockCursor.equalTo(getRawSlice(), valueOffset(offset));
    }

    @Override
    public boolean equals(int position, Slice rightSlice, int rightOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        return type.equals(getRawSlice(), valueOffset(leftEntryOffset), rightSlice, rightOffset);
    }

    @Override
    public int hashCode(int position)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        if (isEntryAtOffsetNull(offset)) {
            return 0;
        }
        return type.hashCode(getRawSlice(), valueOffset(offset));
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock rightBlock, int rightPosition)
    {
        checkReadablePosition(position);
        int leftOffset = getPositionOffset(position);

        boolean leftIsNull = isEntryAtOffsetNull(leftOffset);
        boolean rightIsNull = rightBlock.isNull(rightPosition);

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        // compare the right block to our slice but negate the result since we are evaluating in the opposite order
        int result = -rightBlock.compareTo(rightPosition, getRawSlice(), valueOffset(leftOffset));
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        boolean leftIsNull = isEntryAtOffsetNull(leftEntryOffset);

        boolean rightIsNull = cursor.isNull();

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        // compare the right cursor to our slice but negate the result since we are evaluating in the opposite order
        int result = -cursor.compareTo(getRawSlice(), valueOffset(leftEntryOffset));
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(int position, Slice rightSlice, int rightOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        return type.compareTo(getRawSlice(), valueOffset(leftEntryOffset), rightSlice, rightOffset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        if (isEntryAtOffsetNull(offset)) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(getRawSlice(), valueOffset(offset), blockBuilder);
        }
    }

    private int valueOffset(int entryOffset)
    {
        return entryOffset + SIZE_OF_BYTE;
    }

    private boolean isEntryAtOffsetNull(int offset)
    {
        return getRawSlice().getByte(offset) != 0;
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalStateException("position is not valid");
        }
    }
}
