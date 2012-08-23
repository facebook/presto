package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.primitives.Longs.asList;

public class UncompressedPositionBlock
    implements PositionBlock
{
    private final List<Long> positions;
    private final Range<Long> range;

    public UncompressedPositionBlock(long... positions)
    {
        this(asList(positions));
    }

    public UncompressedPositionBlock(List<Long> positions)
    {
        checkNotNull(positions, "positions is null");
        checkArgument(!positions.isEmpty(), "positions is empty");

        this.positions = ImmutableList.copyOf(positions);

        this.range = Ranges.closed(positions.get(0), positions.get(positions.size() - 1));
    }

    public PositionBlock filter(PositionBlock positionBlock) {
        if (positionBlock.isEmpty()) {
            return EmptyPositionBlock.INSTANCE;
        }

        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (Long position : positions) {
            if (positionBlock.apply(position)) {
                builder.add(position);
            }
        }
        ImmutableList<Long> newPositions = builder.build();
        if (newPositions.isEmpty()) {
            return EmptyPositionBlock.INSTANCE;
        }
        return new UncompressedPositionBlock(newPositions);
    }

    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public int getCount()
    {
        return positions.size();
    }

    @Override
    public boolean isSorted()
    {
        return true;
    }

    @Override
    public boolean isSingleValue()
    {
        return positions.size() == 1;
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return false;
    }

    @Override
    public Iterable<Long> getPositions()
    {
        return positions;
    }

    @Override
    public Range<Long> getRange()
    {
        return range;
    }

    public boolean contains(long position)
    {
        // TODO
        return Iterables.any(positions, equalTo(position));
    }

    @Override
    public boolean apply(Long input)
    {
        return contains(input);
    }
}
