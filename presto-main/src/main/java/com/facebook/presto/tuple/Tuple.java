package com.facebook.presto.tuple;

import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

public class Tuple
        implements TupleReadable
{
    private final Slice slice;
    private final TupleInfo tupleInfo;

    public Tuple(Slice slice, TupleInfo tupleInfo)
    {
        this.slice = slice;
        this.tupleInfo = tupleInfo;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Tuple getTuple()
    {
        return this;
    }

    public Slice getTupleSlice()
    {
        return slice;
    }

    @Override
    public long getLong(int index)
    {
        return tupleInfo.getLong(slice, index);
    }

    @Override
    public double getDouble(int index)
    {
        return tupleInfo.getDouble(slice, index);
    }

    @Override
    public Slice getSlice(int index)
    {
        return tupleInfo.getSlice(slice, index);
    }

    @Override
    public boolean isNull(int index)
    {
        return tupleInfo.isNull(slice, index);
    }

    public int size()
    {
        return tupleInfo.size(slice);
    }

    public void writeTo(SliceOutput out)
    {
        out.writeBytes(slice);
    }

    /**
     * Materializes the tuple values as Java Object.
     * This method is mainly for diagnostics and should not be called in normal query processing.
     */
    public List<Object> toValues()
    {
        ImmutableList.Builder<Object> values = ImmutableList.builder();
        int index = 0;
        for (Type type : tupleInfo.getTypes()) {
            switch (type) {
                case FIXED_INT_64:
                    values.add(getLong(index));
                    break;
                case DOUBLE:
                    values.add(getDouble(index));
                    break;
                case VARIABLE_BINARY:
                    values.add(getSlice(index).toString(UTF_8));
                    break;
            }
            index++;
        }
        return values.build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tuple tuple = (Tuple) o;

        if (!slice.equals(tuple.slice)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = slice.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("slice", slice)
                .add("tupleInfo", tupleInfo)
                .toString();
    }
}
