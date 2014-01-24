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
import com.facebook.presto.block.BlockBuilderStatus;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import static com.facebook.presto.operator.aggregation.VarianceAggregations.LONG_STDDEV_INSTANCE;
import static com.facebook.presto.type.BigintType.BIGINT;

public class TestLongStdDevAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus());
        for (int i = start; i < start + length; i++) {
            blockBuilder.append(i);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return LONG_STDDEV_INSTANCE;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length < 2) {
            return null;
        }

        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = start + i;
        }

        StandardDeviation stdDev = new StandardDeviation();
        return stdDev.evaluate(values);
    }
}
