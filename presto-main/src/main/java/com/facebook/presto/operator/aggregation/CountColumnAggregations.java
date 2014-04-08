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

import com.facebook.presto.operator.aggregation.CountColumnAggregation.CountColumnAccumulator;
import com.facebook.presto.operator.aggregation.CountColumnAggregation.CountColumnGroupedAccumulator;
import com.facebook.presto.operator.aggregation.SimpleAggregationFunction.SimpleAccumulator;
import com.facebook.presto.operator.aggregation.SimpleAggregationFunction.SimpleGroupedAccumulator;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public final class CountColumnAggregations
{
    public static final AggregationFunction COUNT_BOOLEAN_COLUMN = createIsolatedAggregation(BOOLEAN);
    public static final AggregationFunction COUNT_LONG_COLUMN = createIsolatedAggregation(BIGINT);
    public static final AggregationFunction COUNT_DOUBLE_COLUMN = createIsolatedAggregation(DOUBLE);
    public static final AggregationFunction COUNT_STRING_COLUMN = createIsolatedAggregation(VARCHAR);

    private CountColumnAggregations() {}

    private static AggregationFunction createIsolatedAggregation(Type parameterType)
    {
        Class<? extends AggregationFunction> functionClass = IsolatedClass.isolateClass(
                AggregationFunction.class,

                CountColumnAggregation.class,
                SimpleAggregationFunction.class,

                CountColumnGroupedAccumulator.class,
                SimpleGroupedAccumulator.class,

                CountColumnAccumulator.class,
                SimpleAccumulator.class);

        try {
            return functionClass
                    .getConstructor(Type.class)
                    .newInstance(parameterType);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
