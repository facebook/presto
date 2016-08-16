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

import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputOperator
        implements Operator
{
    public static class PartitionedOutputFactory
            implements OutputFactory
    {
        private final Supplier<PartitionFunction> partitionFunctionFactory;
        private final Function<Page, Page> partitionFunctionArgumentExtractor;
        private final OutputBuffer outputBuffer;
        private final OptionalInt nullChannel;
        private final DataSize maxMemory;

        public PartitionedOutputFactory(
                Supplier<PartitionFunction> partitionFunctionFactory,
                Function<Page, Page> partitionFunctionArgumentExtractor,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory)
        {
            this.partitionFunctionFactory = requireNonNull(partitionFunctionFactory, "partitionFunctionFactory is null");
            this.partitionFunctionArgumentExtractor = requireNonNull(partitionFunctionArgumentExtractor, "partitionFunctionArgumentExtractor is null");
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, PlanNodeId planNodeId, List<Type> types, Function<Page, Page> pagePreprocessor)
        {
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    types,
                    pagePreprocessor,
                    partitionFunctionFactory,
                    partitionFunctionArgumentExtractor,
                    nullChannel,
                    outputBuffer,
                    maxMemory);
        }
    }

    public static class PartitionedOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final Function<Page, Page> pagePreprocessor;
        private final Supplier<PartitionFunction> partitionFunctionFactory;
        private final Function<Page, Page> partitionFunctionArgumentExtractor;
        private final OptionalInt nullChannel;
        private final OutputBuffer outputBuffer;
        private final DataSize maxMemory;

        public PartitionedOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> sourceTypes,
                Function<Page, Page> pagePreprocessor,
                Supplier<PartitionFunction> partitionFunctionFactory,
                Function<Page, Page> partitionFunctionArgumentExtractor,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.partitionFunctionFactory = requireNonNull(partitionFunctionFactory, "partitionFunctionFactory is null");
            this.partitionFunctionArgumentExtractor = requireNonNull(partitionFunctionArgumentExtractor, "partitionFunctionArgumentExtractor is null");
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PartitionedOutputOperator.class.getSimpleName());
            return new PartitionedOutputOperator(
                    operatorContext,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunctionFactory.get(),
                    partitionFunctionArgumentExtractor,
                    nullChannel,
                    outputBuffer,
                    maxMemory);
        }

        @Override
        public void close()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunctionFactory,
                    partitionFunctionArgumentExtractor,
                    nullChannel,
                    outputBuffer,
                    maxMemory);
        }
    }

    private final OperatorContext operatorContext;
    private final Function<Page, Page> pagePreprocessor;
    private final PagePartitioner partitionFunction;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private boolean finished;

    public PartitionedOutputOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            Function<Page, Page> pagePreprocessor,
            PartitionFunction partitionFunction,
            Function<Page, Page> partitionFunctionArgumentExtractor,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            DataSize maxMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.partitionFunction = new PagePartitioner(partitionFunction, partitionFunctionArgumentExtractor, nullChannel, outputBuffer, sourceTypes, maxMemory);

        operatorContext.setInfoSupplier(this::getInfo);
        // TODO: We should try to make this more accurate
        // Recalculating the retained size of all the PageBuilders is somewhat expensive,
        // so we only do it once here rather than in addInput(), and assume that the size will be constant.
        operatorContext.getSystemMemoryContext().newLocalMemoryContext().setBytes(this.partitionFunction.getRetainedSizeInBytes());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public PartitionedOutputInfo getInfo()
    {
        return partitionFunction.getInfo();
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        finished = true;
        blocked = partitionFunction.flush(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(isBlocked().isDone(), "output is already blocked");

        if (page.getPositionCount() == 0) {
            return;
        }

        page = pagePreprocessor.apply(page);
        blocked = partitionFunction.partitionPage(page);

        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    private static class PagePartitioner
    {
        private final OutputBuffer outputBuffer;
        private final List<Type> sourceTypes;
        private final PartitionFunction partitionFunction;
        private final Function<Page, Page> partitionFunctionArgumentExtractor;
        private final List<PageBuilder> pageBuilders;
        private final OptionalInt nullChannel; // when present, send the position to every partition if this channel is null.
        private final AtomicLong rowsAdded = new AtomicLong();
        private final AtomicLong pagesAdded = new AtomicLong();

        public PagePartitioner(
                PartitionFunction partitionFunction,
                Function<Page, Page> partitionFunctionArgumentExtractor,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                List<Type> sourceTypes,
                DataSize maxMemory)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionFunctionArgumentExtractor = requireNonNull(partitionFunctionArgumentExtractor, "partitionFunctionArgumentExtractor is null");
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");

            int pageSize = Math.min(PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES, ((int) maxMemory.toBytes()) / partitionFunction.getPartitionCount());
            pageSize = Math.max(1, pageSize);

            ImmutableList.Builder<PageBuilder> pageBuilders = ImmutableList.builder();
            for (int i = 0; i < partitionFunction.getPartitionCount(); i++) {
                pageBuilders.add(PageBuilder.withMaxPageSize(pageSize, sourceTypes));
            }
            this.pageBuilders = pageBuilders.build();
        }

        // Does not include size of SharedBuffer
        public long getRetainedSizeInBytes()
        {
            return pageBuilders.stream()
                    .mapToLong(PageBuilder::getRetainedSizeInBytes)
                    .sum();
        }

        public PartitionedOutputInfo getInfo()
        {
            return new PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get());
        }

        public ListenableFuture<?> partitionPage(Page page)
        {
            requireNonNull(page, "page is null");

            Page partitionFunctionArgs = partitionFunctionArgumentExtractor.apply(page);
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (nullChannel.isPresent() && page.getBlock(nullChannel.getAsInt()).isNull(position)) {
                    for (PageBuilder pageBuilder : pageBuilders) {
                        pageBuilder.declarePosition();

                        for (int channel = 0; channel < sourceTypes.size(); channel++) {
                            Type type = sourceTypes.get(channel);
                            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                        }
                    }
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);

                    PageBuilder pageBuilder = pageBuilders.get(partition);
                    pageBuilder.declarePosition();

                    for (int channel = 0; channel < sourceTypes.size(); channel++) {
                        Type type = sourceTypes.get(channel);
                        type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                    }
                }
            }
            return flush(false);
        }

        public ListenableFuture<?> flush(boolean force)
        {
            // add all full pages to output buffer
            List<ListenableFuture<?>> blockedFutures = new ArrayList<>();
            for (int partition = 0; partition < pageBuilders.size(); partition++) {
                PageBuilder partitionPageBuilder = pageBuilders.get(partition);
                if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                    Page pagePartition = partitionPageBuilder.build();
                    partitionPageBuilder.reset();

                    blockedFutures.add(outputBuffer.enqueue(partition, pagePartition));
                    pagesAdded.incrementAndGet();
                    rowsAdded.addAndGet(pagePartition.getPositionCount());
                }
            }
            ListenableFuture<?> future = Futures.allAsList(blockedFutures);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }
            return future;
        }
    }

    public static class PartitionedOutputInfo
            implements Mergeable<PartitionedOutputInfo>
    {
        private final long rowsAdded;
        private final long pagesAdded;

        @JsonCreator
        public PartitionedOutputInfo(
                @JsonProperty("rowsAdded") long rowsAdded,
                @JsonProperty("pagesAdded") long pagesAdded)
        {
            this.rowsAdded = rowsAdded;
            this.pagesAdded = pagesAdded;
        }

        @JsonProperty
        public long getRowsAdded()
        {
            return rowsAdded;
        }

        @JsonProperty
        public long getPagesAdded()
        {
            return pagesAdded;
        }

        @Override
        public PartitionedOutputInfo mergeWith(PartitionedOutputInfo other)
        {
            return new PartitionedOutputInfo(rowsAdded + other.rowsAdded, pagesAdded + other.pagesAdded);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("rowsAdded", rowsAdded)
                    .add("pagesAdded", pagesAdded)
                    .toString();
        }
    }
}
