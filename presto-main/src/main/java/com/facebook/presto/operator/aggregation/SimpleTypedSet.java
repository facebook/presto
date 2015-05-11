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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.IntBigArray;

import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;

public class SimpleTypedSet
        implements TypedSet
{
    private static final float FILL_RATIO = 0.75f;

    private final Type elementType;
    private final IntBigArray blockPositionByHash = new IntBigArray();
    private final BigWriteableBlock elementBlock;

    private int maxFill;
    private int hashMask;
    private static final int EMPTY_SLOT = -1;

    private boolean containsNullElement;

    public SimpleTypedSet(Type elementType, int expectedSize)
    {
        checkArgument(expectedSize > 0, "expectedSize must be > 0");
        this.elementType = checkNotNull(elementType, "elementType must not be null");
        this.elementBlock = new BigWriteableBlock(elementType, expectedSize);

        int hashSize = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashSize);
        this.hashMask = hashSize - 1;

        blockPositionByHash.ensureCapacity(hashSize);
        for (int i = 0; i < hashSize; i++) {
            blockPositionByHash.set(i, EMPTY_SLOT);
        }

        this.containsNullElement = false;
    }

    @Override
    public long getEstimatedSize()
    {
        return elementBlock.getSizeInBytes() + blockPositionByHash.sizeOf();
    }

    @Override
    public boolean contains(Block block, int position)
    {
        checkNotNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        if (block.isNull(position)) {
            return containsNullElement;
        }
        else {
            return blockPositionByHash.get(getHashPositionOfElement(block, position)) != EMPTY_SLOT;
        }
    }

    @Override
    public void add(Block block, int position)
    {
        checkNotNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        if (block.isNull(position)) {
            containsNullElement = true;
        }
        else {
            int hashPosition = getHashPositionOfElement(block, position);
            if (blockPositionByHash.get(hashPosition) == EMPTY_SLOT) {
                addNewElement(hashPosition, block, position);
            }
        }
    }

    @Override
    public int size()
    {
        return elementBlock.getPositionCount() + (containsNullElement ? 1 : 0);
    }

    /**
     * Get slot position of element at {@code position} of {@code block}
     */
    private int getHashPositionOfElement(Block block, int position)
    {
        int hashPosition = getMaskedHash(hashPosition(elementType, block, position));
        while (true) {
            int blockPosition = blockPositionByHash.get(hashPosition);
            // Doesn't have this element
            if (blockPosition == EMPTY_SLOT) {
                return hashPosition;
            }
            // Already has this element
            else if (positionEqualsPosition(elementType, elementBlock, blockPosition, block, position)) {
                return hashPosition;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private void addNewElement(int hashPosition, Block block, int position)
    {
        elementType.appendTo(block, position, elementBlock);
        blockPositionByHash.set(hashPosition, elementBlock.getPositionCount() - 1);

        // increase capacity, if necessary
        if (elementBlock.getPositionCount() >= maxFill) {
            rehash(maxFill * 2);
        }
    }

    private void rehash(int size)
    {
        int newHashSize = arraySize(size + 1, FILL_RATIO);
        hashMask = newHashSize - 1;
        maxFill = calculateMaxFill(newHashSize);
        blockPositionByHash.ensureCapacity(newHashSize);
        for (int i = 0; i < newHashSize; i++) {
            blockPositionByHash.set(i, EMPTY_SLOT);
        }

        rehashBlock(elementBlock);
    }

    private void rehashBlock(Block block)
    {
        for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
            blockPositionByHash.set(getHashPositionOfElement(block, blockPosition), blockPosition);
        }
    }

    private static int calculateMaxFill(int hashSize)
    {
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        return maxFill;
    }

    private int getMaskedHash(int rawHash)
    {
        return rawHash & hashMask;
    }
}
