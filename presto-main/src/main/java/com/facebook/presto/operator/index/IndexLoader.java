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
package com.facebook.presto.operator.index;

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.PagesIndexBuilderOperator.PagesIndexBuilderOperatorFactory;
import com.facebook.presto.operator.index.UnloadedIndexKeyRecordSet.UnloadedIndexKeyRecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class IndexLoader
{
    private final BlockingQueue<UpdateRequest> updateRequests = new LinkedBlockingQueue<>();

    private final List<Type> outputTypes;
    private final int snapshotOperatorId;
    private final DriverFactory driverFactory;
    private final PlanNodeId sourcePlanNodeId;
    private final PagesIndexBuilderOperatorFactory pagesIndexOutput;
    private final int expectedPositions;
    private final DataSize maxIndexMemorySize;

    private final AtomicReference<TaskContext> taskContextReference = new AtomicReference<>();
    private final List<Integer> indexChannels;
    private final List<Type> types;

    @GuardedBy("this")
    private IndexSnapshotBuilder indexSnapshotBuilder;

    @GuardedBy("this")
    private final AtomicReference<IndexSnapshot> indexSnapshotReference;

    public IndexLoader(List<Integer> indexChannels,
            List<Type> types,
            int snapshotOperatorId,
            DriverFactory driverFactory,
            PagesIndexBuilderOperatorFactory pagesIndexOutput,
            int expectedPositions,
            DataSize maxIndexMemorySize)
    {
        checkArgument(!indexChannels.isEmpty(), "indexChannels must not be empty");
        this.indexChannels = ImmutableList.copyOf(checkNotNull(indexChannels, "indexChannels is null"));
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.outputTypes = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.snapshotOperatorId = snapshotOperatorId;
        this.driverFactory = checkNotNull(driverFactory, "driverFactory is null");
        this.sourcePlanNodeId = Iterables.getOnlyElement(driverFactory.getSourceIds());
        this.pagesIndexOutput = checkNotNull(pagesIndexOutput, "pagesIndexOutput is null");
        this.expectedPositions = checkNotNull(expectedPositions, "expectedPositions is null");
        this.maxIndexMemorySize = maxIndexMemorySize;

        // start with an empty source
        indexSnapshotReference = new AtomicReference<>(new IndexSnapshot(new EmptyLookupSource(types.size()), new EmptyLookupSource(indexChannels.size())));
    }

    // This is a ghetto way to acquire a TaskContext at runtime (unavailable at planning)
    public void setContext(TaskContext taskContext)
    {
        taskContextReference.compareAndSet(null, taskContext);
    }

    public int getChannelCount()
    {
        return outputTypes.size();
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    public IndexSnapshot getIndexSnapshot()
    {
        return indexSnapshotReference.get();
    }

    public IndexSnapshot getIndexSnapshotForKeys(int position, Block[] indexBlocks)
    {
        UpdateRequest myUpdateRequest = new UpdateRequest(position, indexBlocks);
        updateRequests.add(myUpdateRequest);

        synchronized (this) {
            if (!myUpdateRequest.isFinished()) {
                List<UpdateRequest> requests = new ArrayList<>();
                updateRequests.drainTo(requests);

                // make sure we load my request... my request may have been drained by another thread
                // but not completed because of memory constraints
                if (!requests.contains(myUpdateRequest)) {
                     requests.add(myUpdateRequest);
                }

                // attempt to load all requests
                if (batchLoadRequests(requests)) {
                    IndexSnapshot indexSnapshot = indexSnapshotReference.get();
                    for (UpdateRequest request : requests) {
                        request.finished(indexSnapshot);
                    }
                    myUpdateRequest.finished(indexSnapshot);
                }
                else {
                    // load just my request
                    // the other requests were drained but that is ok since each will just load their own request
                    if (batchLoadRequests(ImmutableList.of(myUpdateRequest))) {
                        IndexSnapshot indexSnapshot = indexSnapshotReference.get();
                        myUpdateRequest.finished(indexSnapshot);
                    }
                    else {
                        // the load for this single page was too big, just give up
                        throw new ExceededMemoryLimitException(maxIndexMemorySize, "Index");
                    }
                }
            }
        }

        // return the snapshot from the update request as another thread may have already flushed the request
        return myUpdateRequest.getFinishedIndexSnapshot();
    }

    private synchronized boolean batchLoadRequests(List<UpdateRequest> requests)
    {
        if (indexSnapshotBuilder == null) {
            TaskContext taskContext = taskContextReference.get();
            checkState(taskContext != null, "Task context must be set before index can be built");
            PipelineContext pipelineContext = taskContext.addPipelineContext(false, false);
            indexSnapshotBuilder = new IndexSnapshotBuilder(
                    driverFactory,
                    pipelineContext,
                    sourcePlanNodeId,
                    pagesIndexOutput,
                    indexSnapshotReference,
                    indexChannels,
                    types,
                    snapshotOperatorId,
                    expectedPositions,
                    maxIndexMemorySize);
        }

        return indexSnapshotBuilder.batchLoadRequests(requests);
    }

    @NotThreadSafe
    private static class IndexSnapshotBuilder
    {
        private final DriverFactory driverFactory;
        private final PipelineContext pipelineContext;
        private final PlanNodeId sourcePlanNodeId;
        private final List<Integer> indexChannels;
        private final List<Type> types;
        private final List<Type> indexTypes;
        private final AtomicReference<IndexSnapshot> indexSnapshotReference;

        private final PagesIndexBuilder pagesIndexBuilder;

        private final PagesIndex missingKeysIndex;
        private final List<Integer> missingKeysChannels;

        private LookupSource missingKeys;

        private IndexSnapshotBuilder(DriverFactory driverFactory,
                PipelineContext pipelineContext,
                PlanNodeId sourcePlanNodeId,
                PagesIndexBuilderOperatorFactory pagesIndexOutput,
                AtomicReference<IndexSnapshot> indexSnapshotReference,
                List<Integer> indexChannels,
                List<Type> types,
                int snapshotOperatorId,
                int expectedPositions,
                DataSize maxIndexMemorySize)
        {
            this.driverFactory = driverFactory;
            this.pipelineContext = pipelineContext;
            this.sourcePlanNodeId = sourcePlanNodeId;
            this.indexSnapshotReference = indexSnapshotReference;
            this.indexChannels = indexChannels;
            this.types = types;

            ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
            for (Integer outputIndexChannel : indexChannels) {
                typeBuilder.add(types.get(outputIndexChannel));
            }
            indexTypes = typeBuilder.build();

            // create operator context to track this the memory of the index
            OperatorContext snapshotOperatorContext = pipelineContext.addDriverContext().addOperatorContext(snapshotOperatorId, IndexLoader.class.getSimpleName());

            this.pagesIndexBuilder = new PagesIndexBuilder(pagesIndexOutput.getTypes(), expectedPositions, indexChannels, snapshotOperatorContext, maxIndexMemorySize);
            pagesIndexOutput.setPagesIndexBuilder(pagesIndexBuilder);

            ImmutableList.Builder<Type> missingKeysTypes = ImmutableList.builder();
            ImmutableList.Builder<Integer> missingKeysChannels = ImmutableList.builder();
            for (int i = 0; i < indexChannels.size(); i++) {
                Integer outputIndexChannel = indexChannels.get(i);
                missingKeysTypes.add(pagesIndexOutput.getTypes().get(outputIndexChannel));
                missingKeysChannels.add(i);
            }
            this.missingKeysIndex = new PagesIndex(missingKeysTypes.build(), expectedPositions, snapshotOperatorContext);
            this.missingKeysChannels = missingKeysChannels.build();
            this.missingKeys = new EmptyLookupSource(indexChannels.size());
        }

        private boolean batchLoadRequests(List<UpdateRequest> requests)
        {
            boolean initialIndexIsEmpty = pagesIndexBuilder.isEmpty();
            UnloadedIndexKeyRecordSet unloadedKeysRecordSet = load(requests);
            if (pagesIndexBuilder.isMemoryExceeded()) {
                // If the initial index had some data, flush the index and retry
                if (!initialIndexIsEmpty) {
                    indexSnapshotReference.set(new IndexSnapshot(new EmptyLookupSource(types.size()), new EmptyLookupSource(indexChannels.size())));
                    pagesIndexBuilder.reset();
                    unloadedKeysRecordSet = load(requests);
                }
                // If we are still full, flush and fail the load
                if (pagesIndexBuilder.isMemoryExceeded()) {
                    indexSnapshotReference.set(new IndexSnapshot(new EmptyLookupSource(types.size()), new EmptyLookupSource(indexChannels.size())));
                    pagesIndexBuilder.reset();
                    return false;
                }
            }

            // Create lookup source with new data
            LookupSource lookupSource = pagesIndexBuilder.createLookupSource();

            // Build a page containing the keys that produced no output rows, so in future requests can skip these keys
            PageBuilder missingKeysPageBuilder = new PageBuilder(missingKeysIndex.getTypes());
            UnloadedIndexKeyRecordCursor unloadedKeyRecordCursor = unloadedKeysRecordSet.cursor();
            while (unloadedKeyRecordCursor.advanceNextPosition()) {
                Block[] blocks = unloadedKeyRecordCursor.getBlocks();
                int position = unloadedKeyRecordCursor.getPosition();
                if (lookupSource.getJoinPosition(position, blocks) < 0) {
                    for (int i = 0; i < blocks.length; i++) {
                        Block block = blocks[i];
                        Type type = unloadedKeyRecordCursor.getType(i);
                        type.appendTo(block, position, missingKeysPageBuilder.getBlockBuilder(i));
                    }
                }
            }

            // only update missing keys if we have new missing keys
            if (!missingKeysPageBuilder.isEmpty()) {
                missingKeysIndex.addPage(missingKeysPageBuilder.build());
                missingKeys = missingKeysIndex.createLookupSource(missingKeysChannels);
            }

            IndexSnapshot newValue = new IndexSnapshot(lookupSource, missingKeys);
            indexSnapshotReference.set(newValue);
            return true;
        }

        private UnloadedIndexKeyRecordSet load(List<UpdateRequest> requests)
        {
            UnloadedIndexKeyRecordSet unloadedKeysRecordSet = new UnloadedIndexKeyRecordSet(indexSnapshotReference.get(), indexTypes, requests);

            // Drive index lookup to produce the output (landing in pagesIndexOutput)
            Driver driver = driverFactory.createDriver(pipelineContext.addDriverContext());
            driver.updateSource(new TaskSource(sourcePlanNodeId, ImmutableSet.of(new ScheduledSplit(0, new Split("index", new IndexSplit(unloadedKeysRecordSet)))), true));
            while (!driver.isFinished()) {
                ListenableFuture<?> process = driver.process();
                checkState(process.isDone(), "Driver should never block");
            }
            return unloadedKeysRecordSet;
        }
    }

    private static class EmptyLookupSource
            implements LookupSource
    {
        private final int channelCount;

        private EmptyLookupSource(int channelCount)
        {
            this.channelCount = channelCount;
        }

        @Override
        public int getChannelCount()
        {
            return channelCount;
        }

        @Override
        public long getJoinPosition(int position, Block... blocks)
        {
            return IndexSnapshot.UNLOADED_INDEX_KEY;
        }

        @Override
        public long getNextJoinPosition(long currentPosition)
        {
            return IndexSnapshot.UNLOADED_INDEX_KEY;
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new UnsupportedOperationException();
        }
    }
}
