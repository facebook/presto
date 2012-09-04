package com.facebook.presto.aggregations;

import com.facebook.presto.Cursor;
import com.facebook.presto.PositionBlock;
import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    void add(ValueBlock values, PositionBlock relevantPositions);
    void add(Cursor cursor, Range relevantRange);

    Tuple evaluate();
}
