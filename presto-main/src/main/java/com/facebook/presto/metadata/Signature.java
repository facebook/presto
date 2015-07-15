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
import com.facebook.presto.type.TypeUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeRegistry.canCoerce;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.any;
import static java.util.Objects.requireNonNull;

public final class Signature
{
    private final String name;
    private final FunctionKind kind;
    private final List<TypeParameter> typeParameters;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;

    @JsonCreator
    public Signature(
            @JsonProperty("name") String name,
            @JsonProperty("kind") FunctionKind kind,
            @JsonProperty("typeParameters") List<TypeParameter> typeParameters,
            @JsonProperty("returnType") TypeSignature returnType,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity)
    {
        requireNonNull(name, "name is null");
        requireNonNull(typeParameters, "typeParameters is null");

        this.name = name;
        this.kind = requireNonNull(kind, "type is null");
        this.typeParameters = ImmutableList.copyOf(typeParameters);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.variableArity = variableArity;
    }

    public Signature(String name, FunctionKind kind, List<TypeParameter> typeParameters, String returnType, List<String> argumentTypes, boolean variableArity)
    {
        this(name, kind, typeParameters, parseTypeSignature(returnType), Lists.transform(argumentTypes, TypeSignature::parseTypeSignature), variableArity);
    }

    public Signature(String name, FunctionKind kind, String returnType, List<String> argumentTypes)
    {
        this(name, kind, ImmutableList.<TypeParameter>of(), parseTypeSignature(returnType), Lists.transform(argumentTypes, TypeSignature::parseTypeSignature), false);
    }

    public Signature(String name, FunctionKind kind, String returnType, String... argumentTypes)
    {
        this(name, kind, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public Signature(String name, FunctionKind kind, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        this(name, kind, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false);
    }

    public Signature(String name, FunctionKind kind, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        this(name, kind, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalOperator(OperatorType operator, Type returnType, List<? extends Type> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(operator.name()), returnType.getTypeSignature(), argumentTypes.stream().map(Type::getTypeSignature).collect(toImmutableList()));
    }

    public static Signature internalOperator(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(name), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(name), returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, String returnType, String... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, String returnType, List<String> argumentTypes)
    {
        return new Signature(name, SCALAR, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false);
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, SCALAR, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public FunctionKind getKind()
    {
        return kind;
    }

    @JsonProperty
    public TypeSignature getReturnType()
    {
        return returnType;
    }

    @JsonProperty
    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @JsonProperty
    public boolean isVariableArity()
    {
        return variableArity;
    }

    @JsonProperty
    public List<TypeParameter> getTypeParameters()
    {
        return typeParameters;
    }

    public Signature resolveCalculatedTypes(List<TypeSignature> parameterTypes)
    {
        if (!isReturnTypeOrAnyArgumentTypeCalculated()) {
            return this;
        }

        Map<String, OptionalLong> inputs = bindLiteralParameters(parameterTypes);
        TypeSignature calculatedReturnType = TypeUtils.resolveCalculatedType(returnType, inputs);
        return new Signature(name, kind, calculatedReturnType, parameterTypes);
    }

    public boolean isReturnTypeOrAnyArgumentTypeCalculated()
    {
        return returnType.isCalculated() || any(argumentTypes, TypeSignature::isCalculated);
    }

    public Map<String, OptionalLong> bindLiteralParameters(List<TypeSignature> parameterTypes)
    {
        Map<String, OptionalLong> boundParameters = new HashMap<>();

        for (int index = 0; index < argumentTypes.size(); index++) {
            TypeSignature argument = argumentTypes.get(index);
            if (argument.isCalculated()) {
                TypeSignature actualParameter = parameterTypes.get(index);
                boundParameters.putAll(TypeUtils.extractLiteralParameters(argument, actualParameter));
            }
        }
        return boundParameters;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, kind, typeParameters, returnType, argumentTypes, variableArity);
    }

    Signature withAlias(String name)
    {
        return new Signature(name, kind, typeParameters, getReturnType(), getArgumentTypes(), variableArity);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Signature other = (Signature) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.kind, other.kind) &&
                Objects.equals(this.typeParameters, other.typeParameters) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.variableArity, other.variableArity);
    }

    @Override
    public String toString()
    {
        return name + (typeParameters.isEmpty() ? "" : "<" + Joiner.on(",").join(typeParameters) + ">") + "(" + Joiner.on(",").join(argumentTypes) + "):" + returnType;
    }

    @Nullable
    public Map<String, Type> bindTypeParameters(Type returnType, List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        ImmutableMap.Builder<String, TypeParameter> builder = ImmutableMap.builder();
        for (TypeParameter parameter : typeParameters) {
            builder.put(parameter.getName(), parameter);
        }

        ImmutableMap<String, TypeParameter> parameters = builder.build();
        if (!matchAndBind(boundParameters, parameters, this.returnType, returnType, allowCoercion, typeManager)) {
            return null;
        }

        if (!matchArguments(boundParameters, parameters, argumentTypes, types, allowCoercion, variableArity, typeManager)) {
            return null;
        }

        checkState(boundParameters.keySet().equals(parameters.keySet()),
                "%s matched arguments %s, but type parameters %s are still unbound",
                this,
                types,
                Sets.difference(parameters.keySet(), boundParameters.keySet()));

        return boundParameters;
    }

    @Nullable
    public Map<String, Type> bindTypeParameters(List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        ImmutableMap.Builder<String, TypeParameter> builder = ImmutableMap.builder();
        for (TypeParameter parameter : typeParameters) {
            builder.put(parameter.getName(), parameter);
        }

        ImmutableMap<String, TypeParameter> parameters = builder.build();
        if (!matchArguments(boundParameters, parameters, argumentTypes, types, allowCoercion, variableArity, typeManager)) {
            return null;
        }

        checkState(boundParameters.keySet().equals(parameters.keySet()), "%s matched arguments %s, but type parameters %s are still unbound", this, types, Sets.difference(parameters.keySet(), boundParameters.keySet()));

        return boundParameters;
    }

    private static boolean matchArguments(
            Map<String, Type> boundParameters,
            Map<String, TypeParameter> parameters,
            List<TypeSignature> argumentTypes,
            List<? extends Type> types,
            boolean allowCoercion,
            boolean varArgs,
            TypeManager typeManager)
    {
        if (varArgs) {
            if (types.size() < argumentTypes.size() - 1) {
                return false;
            }
        }
        else {
            if (argumentTypes.size() != types.size()) {
                return false;
            }
        }

        // Bind the variable arity argument first, to make sure it's bound to the common super type
        if (varArgs && types.size() >= argumentTypes.size()) {
            Optional<Type> superType = typeManager.getCommonSuperType(types.subList(argumentTypes.size() - 1, types.size()));
            if (!superType.isPresent()) {
                return false;
            }
            if (!matchAndBind(boundParameters, parameters, argumentTypes.get(argumentTypes.size() - 1), superType.get(), allowCoercion, typeManager)) {
                return false;
            }
        }

        for (int i = 0; i < types.size(); i++) {
            // Get the current argument signature, or the last one, if this is a varargs function
            TypeSignature typeSignature = argumentTypes.get(Math.min(i, argumentTypes.size() - 1));
            Type type = types.get(i);
            if (!matchAndBind(boundParameters, parameters, typeSignature, type, allowCoercion, typeManager)) {
                return false;
            }
        }

        return true;
    }

    private static boolean matchAndBind(Map<String, Type> boundParameters, Map<String, TypeParameter> typeParameters, TypeSignature parameter, Type type, boolean allowCoercion, TypeManager typeManager)
    {
        // TODO: the code flow here needs reworking. The 'if (allowCoercion()) {...
        // does not make much sense if parameter, we are matching to, is calculated type.
        // Similar binding logic as is done for type parameters should be performed here
        // for literal parameters. I would like to leave it out of this patch as
        // the type system code is going to be refactored soon anyway.

        // If this parameter is already bound, then match (with coercion)
        if (boundParameters.containsKey(parameter.getBase())) {
            checkArgument(parameter.getParameters().isEmpty(), "Unexpected parametric type");
            if (allowCoercion && !parameter.isCalculated()) { // see above for explanation of !parameter.isCalculated()
                if (canCoerce(type, boundParameters.get(parameter.getBase()))) {
                    return true;
                }
                else if (canCoerce(boundParameters.get(parameter.getBase()), type) && typeParameters.get(parameter.getBase()).canBind(type)) {
                    // Try to coerce current binding to new candidate
                    boundParameters.put(parameter.getBase(), type);
                    return true;
                }
                else {
                    // Try to use common super type of current binding and candidate
                    Optional<Type> commonSuperType = typeManager.getCommonSuperType(boundParameters.get(parameter.getBase()), type);
                    if (commonSuperType.isPresent() && typeParameters.get(parameter.getBase()).canBind(commonSuperType.get())) {
                        boundParameters.put(parameter.getBase(), commonSuperType.get());
                        return true;
                    }
                }
                return false;
            }
            else {
                return type.equals(boundParameters.get(parameter.getBase()));
            }
        }

        // Recurse into component types
        if (!parameter.getParameters().isEmpty()) {
            if (type.getTypeParameters().size() != parameter.getParameters().size()) {
                return false;
            }
            for (int i = 0; i < parameter.getParameters().size(); i++) {
                Type componentType = type.getTypeParameters().get(i);
                TypeSignature componentSignature = parameter.getParameters().get(i);
                if (!matchAndBind(boundParameters, typeParameters, componentSignature, componentType, allowCoercion, typeManager)) {
                    return false;
                }
            }
        }

        // Bind parameter, if this is a free type parameter
        if (typeParameters.containsKey(parameter.getBase())) {
            TypeParameter typeParameter = typeParameters.get(parameter.getBase());
            if (!typeParameter.canBind(type)) {
                return false;
            }
            boundParameters.put(parameter.getBase(), type);
            return true;
        }

        // We've already checked all the components, so just match the base type
        if (!parameter.getParameters().isEmpty()) {
            return type.getTypeSignature().getBase().equals(parameter.getBase());
        }

        // The parameter is not a type parameter, so it must be a concrete type
        Type parameterType = typeManager.getType(parseTypeSignature(parameter.getBase()));
        if (allowCoercion && !parameter.isCalculated()) { // see above for explanation of !parameter.isCalculated()
            return canCoerce(type, parameterType);
        }
        else {
            return type.getTypeSignature().getBase().equals(parameterType.getTypeSignature().getBase());
        }
    }

    /*
     * similar to T extends MyClass<?...>, if Java supported varargs wildcards
     */
    public static TypeParameter withVariadicBound(String name, String variadicBound)
    {
        return new TypeParameter(name, false, false, variadicBound);
    }

    public static TypeParameter comparableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeParameter(name, true, false, variadicBound);
    }

    public static TypeParameter typeParameter(String name)
    {
        return new TypeParameter(name, false, false, null);
    }

    public static TypeParameter comparableTypeParameter(String name)
    {
        return new TypeParameter(name, true, false, null);
    }

    public static TypeParameter orderableTypeParameter(String name)
    {
        return new TypeParameter(name, false, true, null);
    }

    public static SignatureBuilder builder()
    {
        return new SignatureBuilder();
    }

    public static SignatureBuilder builder(Signature source)
    {
        return new SignatureBuilder(source);
    }
}
