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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.AggregationFunction;
import com.facebook.presto.common.function.CombineFunction;
import com.facebook.presto.common.function.InputFunction;
import com.facebook.presto.common.function.OutputFunction;
import com.facebook.presto.common.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.LongAndDoubleState;

import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static java.lang.Math.round;

@AggregationFunction("avg")
public final class IntervalDayToSecondAverageAggregation
{
    private IntervalDayToSecondAverageAggregation() {}

    @InputFunction
    public static void input(LongAndDoubleState state, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
    }

    @CombineFunction
    public static void combine(LongAndDoubleState state, LongAndDoubleState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @OutputFunction(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static void output(LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double value = state.getDouble();
            INTERVAL_DAY_TIME.writeLong(out, round(value / count));
        }
    }
}
