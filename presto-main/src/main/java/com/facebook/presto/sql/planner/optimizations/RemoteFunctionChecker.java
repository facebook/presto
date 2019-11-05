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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import static com.facebook.presto.spi.function.FunctionImplementationType.REMOTE;
import static java.util.Objects.requireNonNull;

public class RemoteFunctionChecker
        implements RowExpressionVisitor<Boolean, Void>
{
    private final FunctionManager functionManager;

    public RemoteFunctionChecker(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    @Override
    public Boolean visitCall(CallExpression call, Void context)
    {
        FunctionMetadata functionMetadata = functionManager.getFunctionMetadata(call.getFunctionHandle());
        if (functionMetadata.getImplementationType().equals(REMOTE)) {
            return true;
        }
        return call.getArguments().stream().anyMatch(expresison -> expresison.accept(this, null));
    }

    @Override
    public Boolean visitInputReference(InputReferenceExpression reference, Void context)
    {
        return false;
    }

    @Override
    public Boolean visitConstant(ConstantExpression literal, Void context)
    {
        return false;
    }

    @Override
    public Boolean visitLambda(LambdaDefinitionExpression lambda, Void context)
    {
        return false;
    }

    @Override
    public Boolean visitVariableReference(VariableReferenceExpression reference, Void context)
    {
        return false;
    }

    @Override
    public Boolean visitSpecialForm(SpecialFormExpression specialForm, Void context)
    {
        return false;
    }
}
