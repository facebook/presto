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

import com.facebook.presto.operator.aggregation.state.CovarianceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.operator.aggregation.AggregationUtils.countBelowAggregationThreshold;
import static com.facebook.presto.operator.aggregation.AggregationUtils.getCovariancePop;
import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeCovarianceState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateCovarianceState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction(value = "covariance", alias = "covar_samp")
public class CovarianceAggregation
{
    private CovarianceAggregation() {}

    @InputFunction
    public static void input(CovarianceState state, @SqlType(StandardTypes.DOUBLE) double independentValue, @SqlType(StandardTypes.DOUBLE) double dependentValue)
    {
        updateCovarianceState(state, independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(CovarianceState state, CovarianceState otherState)
    {
        mergeCovarianceState(state, otherState);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void covar(CovarianceState state, BlockBuilder out)
    {
        if (countBelowAggregationThreshold(state)) {
            out.appendNull();
        }
        else {
            double covar = getCovariancePop(state) * state.getCount() / (state.getCount() - 1);
            DOUBLE.writeDouble(out, covar);
        }
    }
}
