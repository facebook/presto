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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

public class PageFunctionCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        FunctionRegistry registry = context.getRegistry();

        FunctionInfo function = registry.getExactFunction(signature);
        if (function == null) {
            // TODO: temporary hack to deal with magic timestamp literal functions which don't have an "exact" form and need to be "resolved"
            function = registry.resolveFunction(QualifiedName.of(signature.getName()), signature.getArgumentTypes(), false);
        }

        Preconditions.checkArgument(function != null, "Function %s not found", signature);

        // check if we can use a block direct invocation
        if (function.getBlockMethodHandle() != null && arguments.stream().allMatch(argument -> argument instanceof InputReferenceExpression)) {
            List<Variable> blockVariables = arguments.stream()
                    .map(input -> context.getContext().getVariable("block_" + ((InputReferenceExpression) input).getField()))
                    .collect(Collectors.toList());
            return context.generateBlockCall(function, blockVariables);
        }

        List<ByteCodeNode> argumentsByteCode = arguments.stream()
                .map(context::generate)
                .collect(Collectors.toList());

        return context.generateCall(function, argumentsByteCode);

    }
}
