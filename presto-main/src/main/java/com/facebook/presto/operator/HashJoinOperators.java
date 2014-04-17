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
package com.facebook.presto.operator;

import com.facebook.presto.operator.HashBuilderOperator.HashSupplier;
import com.facebook.presto.sql.gen.JoinProbeCompiler;
import com.facebook.presto.spi.type.Type;

import java.util.List;

public class HashJoinOperators
{
    private HashJoinOperators()
    {
    }

    private static final JoinProbeCompiler JOIN_PROBE_COMPILER = new JoinProbeCompiler();

    public static OperatorFactory innerJoin(int operatorId, HashSupplier hashSupplier, List<? extends Type> probeTypes, List<Integer> probeJoinChannel)
    {
        OperatorFactory operatorFactory = JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, hashSupplier, probeTypes, probeJoinChannel, false);
        return operatorFactory;
    }

    public static OperatorFactory outerJoin(int operatorId, HashSupplier hashSupplier, List<? extends Type> probeTypes, List<Integer> probeJoinChannel)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, hashSupplier, probeTypes, probeJoinChannel, true);
    }
}
