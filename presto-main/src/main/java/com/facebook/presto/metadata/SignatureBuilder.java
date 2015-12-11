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

import com.facebook.presto.spi.type.TypeSignature;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public final class SignatureBuilder
{
    private String name;
    private FunctionKind kind;
    private List<TypeParameter> typeParameters = emptyList();
    private TypeSignature returnType;
    private List<TypeSignature> argumentTypes;
    private boolean variableArity;

    public SignatureBuilder() {}

    public SignatureBuilder(Signature source)
    {
        name = source.getName();
        kind = source.getKind();
        typeParameters = new ArrayList<>(source.getTypeParameters());
        returnType = source.getReturnType();
        argumentTypes = new ArrayList<>(source.getArgumentTypes());
        variableArity = source.isVariableArity();
    }

    public SignatureBuilder name(String name)
    {
        this.name = requireNonNull(name, "name is null");
        return this;
    }

    public SignatureBuilder kind(FunctionKind kind)
    {
        this.kind = kind;
        return this;
    }

    public SignatureBuilder operatorType(OperatorType operatorType)
    {
        this.name = mangleOperatorName(requireNonNull(operatorType, "operatorType is null"));
        this.kind = SCALAR;
        return this;
    }

    public SignatureBuilder typeParameters(TypeParameter... typeParameters)
    {
        return typeParameters(asList(requireNonNull(typeParameters, "typeParameters is null")));
    }

    public SignatureBuilder typeParameters(List<TypeParameter> typeParameters)
    {
        this.typeParameters = new ArrayList<>(requireNonNull(typeParameters, "typeParameters is null"));
        return this;
    }

    public SignatureBuilder clearTypeParameters()
    {
        this.typeParameters = new ArrayList<>();
        return this;
    }

    public SignatureBuilder addTypeParameter(TypeParameter typeParameter)
    {
        this.typeParameters.add(typeParameter);
        return this;
    }

    public SignatureBuilder returnType(String returnType)
    {
        this.returnType = parseTypeSignature(requireNonNull(returnType, "returnType is null"));
        return this;
    }

    public SignatureBuilder argumentTypes(String... argumentTypes)
    {
        return argumentTypes(asList(requireNonNull(argumentTypes, "argumentTypes is Null")));
    }

    public SignatureBuilder argumentTypes(List<String> argumentTypes)
    {
        this.argumentTypes = argumentTypes.stream()
                .map(TypeSignature::parseTypeSignature)
                .collect(toCollection(ArrayList::new));
        return this;
    }

    public SignatureBuilder clearArgumentTypes()
    {
        this.argumentTypes = new ArrayList<>();
        return this;
    }

    public SignatureBuilder addArgumentType(TypeSignature argumentType)
    {
        this.argumentTypes.add(argumentType);
        return this;
    }

    public SignatureBuilder setVariableArity(boolean variableArity)
    {
        this.variableArity = variableArity;
        return this;
    }

    public Signature build()
    {
        return new Signature(name, kind, typeParameters, returnType, argumentTypes, variableArity);
    }
}
