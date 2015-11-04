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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;
import java.util.Map;

public interface ParametricFunction
{
    Signature getSignature();

    boolean isHidden();

    boolean isDeterministic();

    String getDescription();

    // TODO: This should really return an object with just the MethodHandle/InternalAggregation...etc. However, due to the magic literal hack this is not possible
    FunctionInfo specialize(Map<String, Type> types, List<TypeSignature> parameterTypes, TypeManager typeManager, FunctionRegistry functionRegistry);

    static ParametricFunctionBuilder builder(Class<?> clazz)
    {
        return new ParametricFunctionBuilder(clazz);
    }

    static ParametricFunctionBuilder builder()
    {
        return new ParametricFunctionBuilder();
    }
}
