package com.facebook.presto.operator.scalar;
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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayEqualOperator extends ParametricOperator
{
    public static final ArrayEqualOperator ARRAY_EQUAL = new ArrayEqualOperator();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BOOLEAN);

    private ArrayEqualOperator()
    {
        super(EQUAL, ImmutableList.of(comparableWithVariadicBound("T", "array"), comparableWithVariadicBound("T", "array")), StandardTypes.BOOLEAN, ImmutableList.of("T", "T"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        Type type = types.get("T");
        TypeSignature typeSignature = type.getTypeSignature();
        MethodHandle methodHandle = methodHandle(ArrayEqualOperator.class, "equals", Type.class, Slice.class, Slice.class);
        return operatorInfo(EQUAL, RETURN_TYPE, ImmutableList.of(typeSignature, typeSignature), methodHandle.bindTo(type), false, ImmutableList.of(false, false));
    }

    public static boolean equals(Type type, Slice left, Slice right)
    {
        BlockBuilder leftBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        BlockBuilder rightBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        leftBlockBuilder.writeBytes(left, 0, left.length());
        rightBlockBuilder.writeBytes(right, 0, right.length());
        return type.equalTo(leftBlockBuilder.closeEntry().build(), 0, rightBlockBuilder.closeEntry().build(), 0);
    }
}
