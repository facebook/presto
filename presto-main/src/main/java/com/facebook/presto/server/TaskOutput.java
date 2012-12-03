/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.transform;

public class TaskOutput
{
    private final String taskId;
    private final List<TupleInfo> tupleInfos;
    private final Map<String, QueryState> outputBuffers;

    private final AtomicReference<State> taskState = new AtomicReference<>(State.PREPARING);

    private final int splits;
    private final AtomicInteger startedSplits = new AtomicInteger();
    private final AtomicInteger completedSplits = new AtomicInteger();

    private final AtomicLong splitCpuMillis = new AtomicLong();

    private final AtomicLong inputDataSize = new AtomicLong();
    private final AtomicLong inputPositions = new AtomicLong();

    private final AtomicLong completedDataSize = new AtomicLong();
    private final AtomicLong completedPositions = new AtomicLong();


    public TaskOutput(String taskId, List<String> outputIds, List<TupleInfo> tupleInfos, int pageBufferMax, int splits)
    {
        this.taskId = taskId;
        this.tupleInfos = tupleInfos;
        this.splits = splits;
        ImmutableMap.Builder<String, QueryState> builder = ImmutableMap.builder();
        for (String outputId : outputIds) {
            builder.put(outputId, new QueryState(tupleInfos, 1, pageBufferMax));
        }
        outputBuffers = builder.build();
    }

    public String getTaskId()
    {
        return taskId;
    }

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    public State getState()
    {
        // TODO: this method should not have side-effects. Move that logic to a background thread or something
        State overallState = taskState.get();
        Iterable<State> taskStates = transform(outputBuffers.values(), new Function<QueryState, State>()
        {
            @Override
            public State apply(QueryState outputBuffer)
            {
                return outputBuffer.getState();
            }
        });

        if (overallState == State.PREPARING || overallState == State.RUNNING) {
            if (Iterables.any(taskStates, Predicates.equalTo(State.FAILED))) {
                // this shouldn't happen, but be safe
                overallState = State.FAILED;
                taskState.set(overallState);
                cancel();
            }
            else if (Iterables.any(taskStates, Predicates.equalTo(State.CANCELED))) {
                // this shouldn't happen, but be safe
                overallState = State.CANCELED;
                taskState.set(overallState);
                cancel();
            }
            else if (Iterables.all(taskStates, Predicates.equalTo(State.FINISHED))) {
                overallState = State.FINISHED;
                taskState.set(overallState);
            }
        }
        return overallState;
    }

    /**
     * Marks the output as complete.  After this method is called no more data can be added but there may still be buffered output pages.
     */
    public void finish()
    {
        for (QueryState outputBuffer : outputBuffers.values()) {
            outputBuffer.sourceFinished();
        }
        // update overall state
        getState();
    }

    public void cancel()
    {
        while (true) {
            State state = taskState.get();
            if (state != State.PREPARING && state != State.RUNNING) {
                break;
            }
            if (taskState.compareAndSet(state, State.CANCELED)) {
                break;
            }
        }
        for (QueryState outputBuffer : outputBuffers.values()) {
            outputBuffer.cancel();
        }
    }

    public void queryFailed(Throwable cause)
    {
        taskState.set(State.FAILED);
        for (QueryState outputBuffer : outputBuffers.values()) {
            outputBuffer.queryFailed(cause);
        }
    }

    public int getBufferedPageCount()
    {
        int bufferedPageCount = 0;
        for (QueryState outputBuffer : outputBuffers.values()) {
            bufferedPageCount = Math.max(outputBuffer.getBufferedPageCount(), bufferedPageCount);
        }
        return bufferedPageCount;
    }

    public void addPage(Page page)
            throws InterruptedException
    {
        // transition from preparing to running when first page is produced
        taskState.compareAndSet(State.PREPARING, State.RUNNING);

        for (QueryState outputBuffer : outputBuffers.values()) {
            outputBuffer.addPage(page);
        }
    }

    public List<Page> getNextPages(String outputName, int maxPageCount, Duration maxWait)
            throws InterruptedException
    {
        QueryState outputBuffer = outputBuffers.get(outputName);
        Preconditions.checkArgument(outputBuffer != null, "Unknown output %s: available outputs %s", outputName, outputBuffers.keySet());
        return outputBuffer.getNextPages(maxPageCount, maxWait);
    }

    public QueryTaskInfo getQueryTaskInfo()
    {
        return new QueryTaskInfo(taskId,
                getTupleInfos(),
                getState(),
                getBufferedPageCount(),
                getSplits(),
                getStartedSplits(),
                getCompletedSplits(),
                (long) getSplitCpu().toMillis(),
                getInputDataSize().toBytes(),
                getInputPositions(),
                getCompletedDataSize().toBytes(),
                getCompletedPositions(),
                0,
                0);
    }

    public int getSplits()
    {
        return splits;
    }

    public int getStartedSplits()
    {
        return Ints.min(startedSplits.get(), splits);
    }

    public int getCompletedSplits()
    {
        return Ints.min(completedSplits.get(), startedSplits.get(), splits);
    }

    public Duration getSplitCpu()
    {
        return new Duration(splitCpuMillis.get(), TimeUnit.MILLISECONDS);
    }

    public DataSize getInputDataSize()
    {
        return new DataSize(inputDataSize.get(), Unit.BYTE);
    }

    public long getInputPositions()
    {
        return inputPositions.get();
    }

    public DataSize getCompletedDataSize()
    {
        return new DataSize(completedDataSize.get(), Unit.BYTE);
    }

    public long getCompletedPositions()
    {
        return completedPositions.get();
    }

    public void splitStarted()
    {
        startedSplits.incrementAndGet();
    }

    public void splitCompleted()
    {
        completedSplits.incrementAndGet();
    }

    public void addSplitCpuTime(Duration duration)
    {
        splitCpuMillis.addAndGet((long) duration.toMillis());
    }

    public void addInputPositions(long inputPositions)
    {
        this.inputPositions.addAndGet(inputPositions);
    }

    public void addInputDataSize(DataSize inputDataSize)
    {
        this.inputDataSize.addAndGet(inputDataSize.toBytes());
    }

    public void addCompletedPositions(long completedPositions)
    {
        this.completedPositions.addAndGet(completedPositions);
    }

    public void addCompletedDataSize(DataSize completedDataSize)
    {
        this.completedDataSize.addAndGet(completedDataSize.toBytes());
    }
}
