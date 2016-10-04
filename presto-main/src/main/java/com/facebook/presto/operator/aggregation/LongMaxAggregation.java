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

import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.InitialLongValue;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class LongMaxAggregation
        extends AbstractAggregationFunction<LongMaxAggregation.LongMaxState>
{
    public static final LongMaxAggregation LONG_MAX = new LongMaxAggregation();

    public LongMaxAggregation()
    {
        super(BIGINT, BIGINT, BIGINT);
    }

    @Override
    public void processInput(LongMaxState state, BlockCursor cursor)
    {
        state.setNotNull(true);
        state.setLong(Math.max(state.getLong(), cursor.getLong()));
    }

    @Override
    public void evaluateFinal(LongMaxState state, BlockBuilder out)
    {
        if (state.getNotNull()) {
            out.appendLong(state.getLong());
        }
        else {
            out.appendNull();
        }
    }

    public interface LongMaxState
            extends AccumulatorState
    {
        @InitialLongValue(Long.MIN_VALUE)
        long getLong();

        void setLong(long value);

        boolean getNotNull();

        void setNotNull(boolean value);
    }
}
