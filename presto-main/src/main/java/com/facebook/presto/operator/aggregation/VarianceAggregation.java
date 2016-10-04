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

import com.facebook.presto.operator.aggregation.state.VarianceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.operator.aggregation.OnlineVarianceCalculator.mergeState;
import static com.facebook.presto.operator.aggregation.OnlineVarianceCalculator.toSlice;
import static com.facebook.presto.operator.aggregation.OnlineVarianceCalculator.updateState;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class VarianceAggregation
        extends AbstractAggregationFunction<VarianceState>
{
    protected final boolean population;
    protected final boolean inputIsLong;
    protected final boolean standardDeviation;

    public VarianceAggregation(Type parameterType,
            boolean population,
            boolean standardDeviation)
    {
        // Intermediate type should be a fixed width structure
        super(DOUBLE, VARCHAR, parameterType);
        this.population = population;
        if (parameterType == BIGINT) {
            this.inputIsLong = true;
        }
        else if (parameterType == DOUBLE) {
            this.inputIsLong = false;
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT or DOUBLE, but was " + parameterType);
        }
        this.standardDeviation = standardDeviation;
    }

    @Override
    protected void processInput(VarianceState state, BlockCursor cursor)
    {
        double inputValue;
        if (inputIsLong) {
            inputValue = cursor.getLong();
        }
        else {
            inputValue = cursor.getDouble();
        }

        updateState(state, inputValue);
    }

    @Override
    protected void evaluateFinal(VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (population) {
            if (count == 0) {
                out.appendNull();
            }
            else {
                double m2 = state.getM2();
                double result = m2 / count;
                if (standardDeviation) {
                    result = Math.sqrt(result);
                }
                out.appendDouble(result);
            }
        }
        else {
            if (count < 2) {
                out.appendNull();
            }
            else {
                double m2 = state.getM2();
                double result = m2 / (count - 1);
                if (standardDeviation) {
                    result = Math.sqrt(result);
                }
                out.appendDouble(result);
            }
        }
    }

    @Override
    protected void evaluateIntermediate(VarianceState state, BlockBuilder out)
    {
        out.appendSlice(toSlice(state));
    }

    @Override
    protected void processIntermediate(VarianceState state, BlockCursor cursor)
    {
        Slice slice = cursor.getSlice();
        mergeState(state, slice);
    }
}
