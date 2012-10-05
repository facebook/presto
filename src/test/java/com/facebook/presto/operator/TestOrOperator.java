package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.position.UncompressedPositionBlock;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertPositions;
import static org.testng.Assert.assertTrue;

public class TestOrOperator
{
    @Test
    public void testEqual()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(0, 1, 2, 3, 4));
        TupleStream right = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(0, 1, 2, 3, 4));

        OrOperator operator = new OrOperator(left, right);
        Cursor cursor = operator.cursor(new QuerySession());

        assertPositions(cursor, 0, 1, 2, 3, 4);

        assertAdvanceNextPosition(cursor, FINISHED);
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testOverlapping()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(0, 2, 3, 4, 5, 8));
        TupleStream right = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(0, 2, 4, 5, 6));

        OrOperator operator = new OrOperator(left, right);
        Cursor cursor = operator.cursor(new QuerySession());

        assertPositions(cursor, 0, 2, 3, 4, 5, 6, 8);

        assertAdvanceNextPosition(cursor, FINISHED);
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testDisjoint()
            throws Exception
    {
        TupleStream left = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(0, 1, 2, 3, 4));
        TupleStream right = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(5, 6, 7, 8, 9));

        OrOperator operator = new OrOperator(left, right);
        Cursor cursor = operator.cursor(new QuerySession());

        assertPositions(cursor, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertAdvanceNextPosition(cursor, FINISHED);
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testMultipleStreams()
    {
        TupleStream one = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(0, 2, 3, 4, 5, 6));
        TupleStream two = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(0, 1, 2, 4, 5, 7));
        TupleStream three = new GenericTupleStream<>(TupleInfo.EMPTY, new UncompressedPositionBlock(0, 1, 2, 3, 5, 8));

        OrOperator operator = new OrOperator(one, two, three);
        Cursor cursor = operator.cursor(new QuerySession());

        assertPositions(cursor, 0, 1, 2, 3, 4, 5, 6, 7, 8);

        assertAdvanceNextPosition(cursor, FINISHED);
        assertTrue(cursor.isFinished());

    }
}
