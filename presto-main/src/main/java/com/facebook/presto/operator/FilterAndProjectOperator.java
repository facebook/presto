/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class FilterAndProjectOperator
        extends AbstractFilterAndProjectOperator
{
    private final FilterFunction filter;
    private final List<ProjectionFunction> projections;

    public FilterAndProjectOperator(Operator source, FilterFunction filter, ProjectionFunction... projections)
    {
        this(source, filter, ImmutableList.copyOf(projections));
    }

    public FilterAndProjectOperator(Operator source, FilterFunction filter, List<ProjectionFunction> projections)
    {
        super(toTupleInfos(projections), source);
        this.filter = filter;
        this.projections = ImmutableList.copyOf(projections);
    }

    private static ImmutableList<TupleInfo> toTupleInfos(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        return tupleInfos.build();
    }

    @Override
    protected PageIterator iterator(PageIterator source, OperatorStats operatorStats)
    {
        return new FilterAndProjectIterator(source, filter, projections, operatorStats);
    }

    public static class FilterAndProjectIterator
            extends AbstractFilterAndProjectIterator
    {
        private final FilterFunction filterFunction;
        private final List<ProjectionFunction> projections;
        private final OperatorStats operatorStats;
        private long currentCompletedSize;

        public FilterAndProjectIterator(PageIterator pageIterator, FilterFunction filterFunction, List<ProjectionFunction> projections, OperatorStats operatorStats)
        {
            super(toTupleInfos(projections), pageIterator, operatorStats);
            this.filterFunction = filterFunction;
            this.projections = projections;
            this.operatorStats = operatorStats;
        }

        protected void filterAndProjectRowOriented(Block[] blocks, PageBuilder pageBuilder)
        {
            int rows = blocks[0].getPositionCount();

            BlockCursor[] cursors = new BlockCursor[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                cursors[i] = blocks[i].cursor();
            }

            for (int position = 0; position < rows; position++) {
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceNextPosition());
                }

                if (filterFunction.filter(cursors)) {
                    pageBuilder.declarePosition();
                    for (int i = 0; i < projections.size(); i++) {
                        // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                        projections.get(i).project(cursors, pageBuilder.getBlockBuilder(i));
                    }
                }
            }

            for (BlockCursor cursor : cursors) {
                checkState(!cursor.advanceNextPosition());
            }
        }

        @Override
        protected void filterAndProjectRowOriented(RecordCursor cursor, PageBuilder pageBuilder)
        {
            long completedPositions = 0;
            while (!pageBuilder.isFull() && cursor.advanceNextPosition()) {
                completedPositions++;
                if (filterFunction.filter(cursor)) {
                    pageBuilder.declarePosition();
                    for (int i = 0; i < projections.size(); i++) {
                        // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                        projections.get(i).project(cursor, pageBuilder.getBlockBuilder(i));
                    }
                }
            }

            long completedDataSize = cursor.getCompletedBytes();
            if (completedDataSize > currentCompletedSize) {
                operatorStats.addCompletedDataSize(completedDataSize - currentCompletedSize);
                currentCompletedSize = completedDataSize;
            }
            operatorStats.addCompletedPositions(completedPositions);
        }
    }
}

