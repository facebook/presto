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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.ConstantPageProjection;
import com.facebook.presto.operator.project.GeneratedPageProjection;
import com.facebook.presto.operator.project.InputChannels;
import com.facebook.presto.operator.project.InputPageProjection;
import com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProjection;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.DeterminismEvaluator;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Reflection.constructorMethodHandle;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static java.util.Objects.requireNonNull;

public class PageFunctionCompiler
{
    private final Metadata metadata;
    private final DeterminismEvaluator determinismEvaluator;

    private final LoadingCache<RowExpression, Supplier<PageProjection>> projectionCache;
    private final LoadingCache<RowExpression, Supplier<PageFilter>> filterCache;

    private final CacheStatsMBean projectionCacheStats;
    private final CacheStatsMBean filterCacheStats;

    @Inject
    public PageFunctionCompiler(Metadata metadata, CompilerConfig config)
    {
        this(metadata, requireNonNull(config, "config is null").getExpressionCacheSize());
    }

    public PageFunctionCompiler(Metadata metadata, int expressionCacheSize)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.determinismEvaluator = new DeterminismEvaluator(metadata.getFunctionRegistry());

        if (expressionCacheSize > 0) {
            projectionCache = CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(expressionCacheSize)
                    .build(CacheLoader.from(projection -> compileProjectionInternal(projection, Optional.empty())));
            projectionCacheStats = new CacheStatsMBean(projectionCache);
        }
        else {
            projectionCache = null;
            projectionCacheStats = null;
        }

        if (expressionCacheSize > 0) {
            filterCache = CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(expressionCacheSize)
                    .build(CacheLoader.from(filter -> compileFilterInternal(filter, Optional.empty())));
            filterCacheStats = new CacheStatsMBean(filterCache);
        }
        else {
            filterCache = null;
            filterCacheStats = null;
        }
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getProjectionCache()
    {
        return projectionCacheStats;
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getFilterCache()
    {
        return filterCacheStats;
    }

    public Supplier<PageProjection> compileProjection(RowExpression projection, Optional<String> classNameSuffix)
    {
        if (projectionCache == null) {
            return compileProjectionInternal(projection, classNameSuffix);
        }
        return projectionCache.getUnchecked(projection);
    }

    private Supplier<PageProjection> compileProjectionInternal(RowExpression projection, Optional<String> classNameSuffix)
    {
        requireNonNull(projection, "projection is null");

        if (projection instanceof InputReferenceExpression) {
            InputReferenceExpression input = (InputReferenceExpression) projection;
            InputPageProjection projectionFunction = new InputPageProjection(input.getField(), input.getType());
            return () -> projectionFunction;
        }

        if (projection instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) projection;
            ConstantPageProjection projectionFunction = new ConstantPageProjection(constant.getValue(), constant.getType());
            return () -> projectionFunction;
        }

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(projection);

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        // generate Work
        ClassDefinition pageProjectionWorkDefinition = definePageProjectWorkClass(result.getRewrittenExpression(), callSiteBinder, classNameSuffix);

        Class<? extends Work> pageProjectionWorkClass;
        try {
            pageProjectionWorkClass = defineClass(pageProjectionWorkDefinition, Work.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, e);
        }

        return () -> new GeneratedPageProjection(
                result.getRewrittenExpression(),
                determinismEvaluator.isDeterministic(result.getRewrittenExpression()),
                result.getInputChannels(),
                constructorMethodHandle(pageProjectionWorkClass, BlockBuilder.class, ConnectorSession.class, DriverYieldSignal.class, Page.class, SelectedPositions.class, ExpressionProfiler.class));
    }

    private static ParameterizedType generateProjectionWorkClassName(Optional<String> classNameSuffix)
    {
        return makeClassName("PageProjectionWork", classNameSuffix);
    }

    private ClassDefinition definePageProjectWorkClass(RowExpression projection, CallSiteBinder callSiteBinder, Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateProjectionWorkClassName(classNameSuffix),
                type(Object.class),
                type(Work.class));

        FieldDefinition expressionProfilerField = classDefinition.declareField(a(PRIVATE), "expressionProfiler", ExpressionProfiler.class);
        FieldDefinition blockBuilderField = classDefinition.declareField(a(PRIVATE), "blockBuilder", BlockBuilder.class);
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE), "session", ConnectorSession.class);
        FieldDefinition yieldSignalField = classDefinition.declareField(a(PRIVATE), "yieldSignal", DriverYieldSignal.class);
        FieldDefinition pageField = classDefinition.declareField(a(PRIVATE), "page", Page.class);
        FieldDefinition selectedPositionsField = classDefinition.declareField(a(PRIVATE), "selectedPositions", SelectedPositions.class);
        FieldDefinition nextIndexOrPositionField = classDefinition.declareField(a(PRIVATE), "nextIndexOrPosition", int.class);
        FieldDefinition resultField = classDefinition.declareField(a(PRIVATE), "result", Block.class);

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        // process
        generateProcessMethod(
                classDefinition,
                blockBuilderField,
                selectedPositionsField,
                resultField);

        // processPositions
        generateProcessPositionsMethod(
                classDefinition,
                expressionProfilerField,
                sessionField,
                yieldSignalField,
                pageField,
                selectedPositionsField,
                nextIndexOrPositionField);

        // processRange
        generateProcessRangeMethod(
                classDefinition,
                expressionProfilerField,
                sessionField,
                yieldSignalField,
                pageField,
                selectedPositionsField,
                nextIndexOrPositionField);

        // getResult
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "getResult", type(Object.class), ImmutableList.of());
        method.getBody().append(method.getThis().getField(resultField)).ret(Object.class);

        // evaluate
        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, projection);
        generateEvaluateMethod(classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, projection, blockBuilderField);

        // constructor
        Parameter blockBuilder = arg("blockBuilder", BlockBuilder.class);
        Parameter session = arg("session", ConnectorSession.class);
        Parameter yieldSignal = arg("yieldSignal", DriverYieldSignal.class);
        Parameter page = arg("page", Page.class);
        Parameter selectedPositions = arg("selectedPositions", SelectedPositions.class);
        Parameter expressionProfiler = arg("expressionProfiler", ExpressionProfiler.class);

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), blockBuilder, session, yieldSignal, page, selectedPositions, expressionProfiler);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class)
                .append(thisVariable.setField(blockBuilderField, blockBuilder))
                .append(thisVariable.setField(sessionField, session))
                .append(thisVariable.setField(yieldSignalField, yieldSignal))
                .append(thisVariable.setField(pageField, page))
                .append(thisVariable.setField(selectedPositionsField, selectedPositions))
                .append(thisVariable.setField(nextIndexOrPositionField, selectedPositions.invoke("getOffset", int.class)))
                .append(thisVariable.setField(resultField, constantNull(Block.class)))
                .append(thisVariable.setField(expressionProfilerField, expressionProfiler))
                .append(thisVariable.getField(expressionProfilerField).invoke("reset", void.class));

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();

        return classDefinition;
    }

    private static MethodDefinition generateProcessMethod(
            ClassDefinition classDefinition,
            FieldDefinition blockBuilder,
            FieldDefinition selectedPositions,
            FieldDefinition result)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(boolean.class), ImmutableList.of());
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        body.append(new IfStatement()
                .condition(thisVariable.getField(selectedPositions).invoke("isList", boolean.class))
                .ifTrue(new BytecodeBlock()
                        .append(new IfStatement()
                                .condition(not(thisVariable.invoke("processPositions", boolean.class)))
                                .ifTrue(new BytecodeBlock().push(false).retBoolean())))
                .ifFalse(new IfStatement()
                        .condition(not(thisVariable.invoke("processRange", boolean.class)))
                        .ifTrue(new BytecodeBlock().push(false).retBoolean())));

        body.comment("result = this.blockBuilder.build(); return true;")
                .append(thisVariable.setField(result, thisVariable.getField(blockBuilder).invoke("build", Block.class)))
                .push(true)
                .retBoolean();

        return method;
    }

    private void generateProcessRangeMethod(
            ClassDefinition classDefinition,
            FieldDefinition expressionProfiler,
            FieldDefinition session,
            FieldDefinition yieldSignal,
            FieldDefinition page,
            FieldDefinition selectedPositions,
            FieldDefinition nextIndexOrPosition)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "processRange", type(boolean.class), ImmutableList.of());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable thisVariable = method.getThis();
        Variable from = scope.declareVariable("from", body, thisVariable.getField(nextIndexOrPosition));
        Variable to = scope.declareVariable("to", body, add(thisVariable.getField(selectedPositions).invoke("getOffset", int.class), thisVariable.getField(selectedPositions).invoke("size", int.class)));
        Variable index = scope.declareVariable(int.class, "index");

        body.append(new IfStatement()
                .condition(thisVariable.getField(expressionProfiler).invoke("isDoneProfiling", boolean.class))
                .ifTrue(generateDoneProfilingBody(
                        thisVariable,
                        expressionProfiler,
                        page,
                        session,
                        yieldSignal,
                        nextIndexOrPosition,
                        from,
                        to,
                        Optional.empty(),
                        index))
                .ifFalse(new BytecodeBlock()
                        .append(new ForLoop("range loop")
                                .initialize(index.set(from))
                                .condition(lessThan(index, to))
                                .update(index.increment())
                                .body(new BytecodeBlock()
                                        .comment("evaluate()")
                                        .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(session), thisVariable.getField(page), index))
                                        .comment("expressionProfiler.profile()")
                                        .append(thisVariable.getField(expressionProfiler).invoke("profile", void.class))
                                        .comment("check yield")
                                        .append(generateCheckYieldBlock(thisVariable, index, yieldSignal, nextIndexOrPosition))))));

        body.comment("return true;")
                .push(true)
                .retBoolean();
    }

    private void generateProcessPositionsMethod(
            ClassDefinition classDefinition,
            FieldDefinition expressionProfiler,
            FieldDefinition session,
            FieldDefinition yieldSignal,
            FieldDefinition page,
            FieldDefinition selectedPositions,
            FieldDefinition nextIndexOrPosition)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "processPositions", type(boolean.class), ImmutableList.of());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable thisVariable = method.getThis();
        Variable positions = scope.declareVariable(int[].class, "positions");
        Variable index = scope.declareVariable(int.class, "index");
        Variable from = scope.declareVariable("from", body, thisVariable.getField(nextIndexOrPosition));
        Variable to = scope.declareVariable("to", body, add(thisVariable.getField(selectedPositions).invoke("getOffset", int.class), thisVariable.getField(selectedPositions).invoke("size", int.class)));

        BytecodeExpression invokeEvaluate = thisVariable.invoke("evaluate", void.class, thisVariable.getField(session), thisVariable.getField(page), positions.getElement(index));

        body.append((positions.set(thisVariable.getField(selectedPositions).invoke("getPositions", int[].class))));

        body.append(new IfStatement()
                .condition(thisVariable.getField(expressionProfiler).invoke("isDoneProfiling", boolean.class))
                .ifTrue(generateDoneProfilingBody(
                        thisVariable,
                        expressionProfiler,
                        page,
                        session,
                        yieldSignal,
                        nextIndexOrPosition,
                        from,
                        to,
                        Optional.of(positions),
                        index))
                .ifFalse(new BytecodeBlock()
                        .append(new ForLoop("positions loop")
                                .initialize(index.set(from))
                                .condition(lessThan(index, to))
                                .update(index.increment())
                                .body(new BytecodeBlock()
                                        .comment("evaluate()")
                                        .append(invokeEvaluate)
                                        .comment("expressionProfiler.profiler()")
                                        .append(thisVariable.getField(expressionProfiler).invoke("profile", void.class))
                                        .comment("check for yield")
                                        .append(generateCheckYieldBlock(thisVariable, index, yieldSignal, nextIndexOrPosition))))));

        body.comment("return true;")
            .push(true)
            .retBoolean();
    }

    private static BytecodeNode generateDoneProfilingBody(
            Variable thisVariable,
            FieldDefinition expressionProfiler,
            FieldDefinition page,
            FieldDefinition session,
            FieldDefinition yieldSignal,
            FieldDefinition nextIndexOrPosition,
            Variable from,
            Variable to,
            Optional<Variable> positions,
            Variable index)
    {
        return new IfStatement()
                .condition(thisVariable.getField(expressionProfiler).invoke("isExpressionExpensive", boolean.class))
                .ifTrue(new ForLoop("loop checking the yield signal")
                .initialize(index.set(from))
                .condition(lessThan(index, to))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(session), thisVariable.getField(page), positions.map(positionsArray -> positionsArray.getElement(index)).orElse(index)))
                        .append(generateCheckYieldBlock(thisVariable, index, yieldSignal, nextIndexOrPosition))))
                .ifFalse(new ForLoop("loop that doesn't check the yield signal")
                .initialize(index.set(from))
                .condition(lessThan(index, to))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(session), thisVariable.getField(page), positions.map(positionsArray -> positionsArray.getElement(index)).orElse(index)))));
    }

    private static BytecodeBlock generateCheckYieldBlock(Variable thisVariable, Variable index, FieldDefinition yieldSignal, FieldDefinition nextIndexOrPosition)
    {
        return new BytecodeBlock()
                .comment("if (yieldSignal.isSet())")
                .append(new IfStatement()
                        .condition(thisVariable.getField(yieldSignal).invoke("isSet", boolean.class))
                        .ifTrue(new BytecodeBlock()
                                .comment("nextIndexOrPosition = index + 1;")
                                .append(thisVariable.setField(nextIndexOrPosition, add(index, constantInt(1))))
                                .comment("return false;")
                                .push(false)
                                .retBoolean()));
    }

    private MethodDefinition generateEvaluateMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            RowExpression projection,
            FieldDefinition blockBuilder)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "evaluate",
                type(void.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Projection: %s", projection.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        declareBlockVariables(projection, page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata.getFunctionRegistry(),
                compiledLambdaMap);

        body.append(thisVariable.getField(blockBuilder))
                .append(compiler.compile(projection, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.getType()))
                .ret();
        return method;
    }

    public Supplier<PageFilter> compileFilter(RowExpression filter, Optional<String> classNameSuffix)
    {
        if (filterCache == null) {
            return compileFilterInternal(filter, classNameSuffix);
        }
        return filterCache.getUnchecked(filter);
    }

    private Supplier<PageFilter> compileFilterInternal(RowExpression filter, Optional<String> classNameSuffix)
    {
        requireNonNull(filter, "filter is null");

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(filter);

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = defineFilterClass(result.getRewrittenExpression(), result.getInputChannels(), callSiteBinder, classNameSuffix);

        Class<? extends PageFilter> functionClass;
        try {
            functionClass = defineClass(classDefinition, PageFilter.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, filter.toString(), e.getCause());
        }

        return () -> {
            try {
                return functionClass.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new PrestoException(COMPILER_ERROR, e);
            }
        };
    }

    private static ParameterizedType generateFilterClassName(Optional<String> classNameSuffix)
    {
        return makeClassName(PageFilter.class.getSimpleName(), classNameSuffix);
    }

    private ClassDefinition defineFilterClass(RowExpression filter, InputChannels inputChannels, CallSiteBinder callSiteBinder, Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateFilterClassName(classNameSuffix),
                type(Object.class),
                type(PageFilter.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, filter);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, filter);

        FieldDefinition selectedPositions = classDefinition.declareField(a(PRIVATE), "selectedPositions", boolean[].class);
        generatePageFilterMethod(classDefinition, selectedPositions);

        // isDeterministic
        classDefinition.declareMethod(a(PUBLIC), "isDeterministic", type(boolean.class))
                .getBody()
                .append(constantBoolean(determinismEvaluator.isDeterministic(filter)))
                .retBoolean();

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(inputChannels, InputChannels.class), "getInputChannels"))
                .retObject();

        // toString
        String toStringResult = toStringHelper(classDefinition.getType()
                .getJavaClassName())
                .add("filter", filter)
                .toString();
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                // bind constant via invokedynamic to avoid constant pool issues due to large strings
                .append(invoke(callSiteBinder.bind(toStringResult, String.class), "toString"))
                .retObject();

        // constructor
        generateConstructor(classDefinition, cachedInstanceBinder, compiledLambdaMap, method -> {
            Variable thisVariable = method.getScope().getThis();
            method.getBody().append(thisVariable.setField(selectedPositions, newArray(type(boolean[].class), 0)));
        });

        return classDefinition;
    }

    private static MethodDefinition generatePageFilterMethod(ClassDefinition classDefinition, FieldDefinition selectedPositionsField)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(SelectedPositions.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .build());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));

        body.append(new IfStatement("grow selectedPositions if necessary")
                .condition(lessThan(thisVariable.getField(selectedPositionsField).length(), positionCount))
                .ifTrue(thisVariable.setField(selectedPositionsField, newArray(type(boolean[].class), positionCount))));

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.getField(selectedPositionsField));
        Variable position = scope.declareVariable(int.class, "position");
        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(selectedPositions.setElement(position, thisVariable.invoke("filter", boolean.class, session, page, position))));

        body.append(invokeStatic(
                PageFilter.class,
                "positionsArrayToSelectedPositions",
                SelectedPositions.class,
                selectedPositions,
                positionCount)
                .ret());

        return method;
    }

    private MethodDefinition generateFilterMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            RowExpression filter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Filter: %s", filter.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(filter, page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata.getFunctionRegistry(),
                compiledLambdaMap);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(compiler.compile(filter, scope))
                // store result so we can check for null
                .putVariable(result)
                .append(and(not(wasNullVariable), result).ret());
        return method;
    }

    private Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambda(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression expression)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = ImmutableSet.copyOf(extractLambdaExpressions(expression));
        ImmutableMap.Builder<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = ImmutableMap.builder();

        int counter = 0;
        for (LambdaDefinitionExpression lambdaExpression : lambdaExpressions) {
            CompiledLambda compiledLambda = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                    lambdaExpression,
                    "lambda_" + counter,
                    containerClassDefinition,
                    compiledLambdaMap.build(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    metadata.getFunctionRegistry());
            compiledLambdaMap.put(lambdaExpression, compiledLambda);
            counter++;
        }

        return compiledLambdaMap.build();
    }

    private static void generateConstructor(
            ClassDefinition classDefinition,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            Consumer<MethodDefinition> additionalStatements)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        additionalStatements.accept(constructorDefinition);

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }

    private static void declareBlockVariables(RowExpression expression, Parameter page, Scope scope, BytecodeBlock body)
    {
        for (int channel : getInputChannels(expression)) {
            scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
        }
    }

    private static List<Integer> getInputChannels(Iterable<RowExpression> expressions)
    {
        TreeSet<Integer> channels = new TreeSet<>();
        for (RowExpression expression : Expressions.subExpressions(expressions)) {
            if (expression instanceof InputReferenceExpression) {
                channels.add(((InputReferenceExpression) expression).getField());
            }
        }
        return ImmutableList.copyOf(channels);
    }

    private static List<Integer> getInputChannels(RowExpression expression)
    {
        return getInputChannels(ImmutableList.of(expression));
    }

    private static List<Parameter> toBlockParameters(List<Integer> inputChannels)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int channel : inputChannels) {
            parameters.add(arg("block_" + channel, Block.class));
        }
        return parameters.build();
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler(CallSiteBinder callSiteBinder)
    {
        return new InputReferenceCompiler(
                (scope, field) -> scope.getVariable("block_" + field),
                (scope, field) -> scope.getVariable("position"),
                callSiteBinder);
    }
}
