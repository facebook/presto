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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.util.array.BooleanBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class LongMinAggregation
        extends SimpleAggregationFunction
{
    public static final LongMinAggregation LONG_MIN = new LongMinAggregation();

    public LongMinAggregation()
    {
        super(BIGINT, BIGINT, BIGINT);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // Min/max are not effected by distinct, so ignore it.
        checkArgument(confidence == 1.0, "min does not support approximate queries");
        return new LongMinGroupedAccumulator(valueChannel);
    }

    public static class LongMinGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final BooleanBigArray notNull;
        private final LongBigArray minValues;

        public LongMinGroupedAccumulator(int valueChannel)
        {
            super(valueChannel, BIGINT, BIGINT, Optional.<Integer>absent(), Optional.<Integer>absent());

            this.notNull = new BooleanBigArray();

            this.minValues = new LongBigArray(Long.MAX_VALUE);
        }

        @Override
        public long getEstimatedSize()
        {
            return notNull.sizeOf() + minValues.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            notNull.ensureCapacity(groupIdsBlock.getGroupCount());
            minValues.ensureCapacity(groupIdsBlock.getGroupCount(), Long.MAX_VALUE);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                long groupId = groupIdsBlock.getGroupId(position);

                if (!values.isNull()) {
                    notNull.set(groupId, true);

                    long value = values.getLong();
                    value = Math.min(value, minValues.get(groupId));
                    minValues.set(groupId, value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            if (notNull.get((long) groupId)) {
                long value = minValues.get((long) groupId);
                output.appendLong(value);
            }
            else {
                output.appendNull();
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // Min/max are not effected by distinct, so ignore it.
        checkArgument(confidence == 1.0, "min does not support approximate queries");
        return new LongMinAccumulator(valueChannel);
    }

    public static class LongMinAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private long min = Long.MAX_VALUE;

        public LongMinAccumulator(int valueChannel)
        {
            super(valueChannel, BIGINT, BIGINT, Optional.<Integer>absent(), Optional.<Integer>absent());
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull()) {
                    notNull = true;
                    min = Math.min(min, values.getLong());
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (notNull) {
                out.appendLong(min);
            }
            else {
                out.appendNull();
            }
        }
    }
}
