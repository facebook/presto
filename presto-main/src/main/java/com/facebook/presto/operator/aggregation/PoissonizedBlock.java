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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.commons.math3.random.RandomDataGenerator;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

class PoissonizedBlock
        implements Block
{
    private final Block delegate;
    private final long seed;

    public PoissonizedBlock(Block delegate, long seed)
    {
        this.delegate = delegate;
        this.seed = seed;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return SINGLE_LONG;
    }

    @Override
    public int getPositionCount()
    {
        return delegate.getPositionCount();
    }

    @Override
    public DataSize getDataSize()
    {
        return delegate.getDataSize();
    }

    @Override
    public BlockCursor cursor()
    {
        return new PoissonizedBlockCursor(delegate.cursor(), seed);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        throw new UnsupportedOperationException("Poissonized blocks cannot be serialized");
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException("getRegion for poissonized block is not supported");
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        throw new UnsupportedOperationException("Random access to poissonized block is not supported");
    }

    public static class PoissonizedBlockCursor
            implements BlockCursor
    {
        private final RandomDataGenerator rand = new RandomDataGenerator();
        private final BlockCursor delegate;
        private long currentValue;

        private PoissonizedBlockCursor(BlockCursor delegate, long seed)
        {
            checkArgument(delegate.getTupleInfo().equals(SINGLE_LONG), "delegate must be a cursor of longs");
            this.delegate = delegate;
            rand.reSeed(seed);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return SINGLE_LONG;
        }

        @Override
        public int getRemainingPositions()
        {
            return delegate.getRemainingPositions();
        }

        @Override
        public boolean isValid()
        {
            return delegate.isValid();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public boolean advanceNextPosition()
        {
            boolean advanced = delegate.advanceNextPosition();
            if (advanced) {
                currentValue = rand.nextPoisson(delegate.getLong());
            }
            return advanced;
        }

        @Override
        public boolean advanceToPosition(int position)
        {
            // We can't just delegate this method, because for this to be consistent with calling advanceNextPosition() position times, we need to advance the random number generator
            boolean advanced = false;
            while (getPosition() < position) {
                advanced = advanceNextPosition();
                if (!advanced) {
                    break;
                }
            }
            return advanced;
        }

        @Override
        public Block getRegionAndAdvance(int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Tuple getTuple()
        {
            return SINGLE_LONG.builder().append(currentValue).build();
        }

        @Override
        public boolean getBoolean()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong()
        {
            return currentValue;
        }

        @Override
        public double getDouble()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice getSlice()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull()
        {
            checkState(!delegate.isNull(), "delegate to poissonized cursor returned a null row");
            return false;
        }

        @Override
        public int getPosition()
        {
            return delegate.getPosition();
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            return value.getTupleInfo().equals(SINGLE_LONG) && value.getLong() == currentValue;
        }

        @Override
        public int getRawOffset()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice getRawSlice()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTupleTo(BlockBuilder blockBuilder)
        {
            blockBuilder.append(currentValue);
        }
    }
}
