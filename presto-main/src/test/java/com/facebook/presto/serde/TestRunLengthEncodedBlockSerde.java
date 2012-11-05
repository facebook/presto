/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.util.Range;
import com.facebook.presto.tuple.Tuples;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static com.facebook.presto.block.BlockAssertions.blockIterableBuilder;
import static com.facebook.presto.serde.RunLengthEncodedBlockSerde.RLE_BLOCK_SERDE;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRunLengthEncodedBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        RunLengthEncodedBlock expectedBlock = new RunLengthEncodedBlock(Tuples.createTuple("alice"), Range.create(20, 30));

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        RLE_BLOCK_SERDE.writeBlock(sliceOutput, expectedBlock);
        RunLengthEncodedBlock actualBlock = RLE_BLOCK_SERDE.readBlock(sliceOutput.slice().getInput(), SINGLE_VARBINARY, 0);
        assertEquals(actualBlock.getSingleValue(), expectedBlock.getSingleValue());
        BlockAssertions.assertBlockEquals(actualBlock, expectedBlock);
    }

    @Test
    public void testCreateBlockWriter()
    {
        BlockIterable blocks = blockIterableBuilder(0, SINGLE_VARBINARY)
                .append("alice")
                .append("alice")
                .append("bob")
                .append("bob")
                .newBlock()
                .append("bob")
                .append("bob")
                .append("charlie")
                .newBlock()
                .append("charlie")
                .append("charlie")
                .newBlock()
                .append("charlie")
                .append("charlie")
                .append("charlie")
                .build();

        SimpleBlocksSerde blocksSerde = new SimpleBlocksSerde(RLE_BLOCK_SERDE);
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blocksSerde.writeBlocks(sliceOutput, blocks);
        BlockIterable actualBlocks = blocksSerde.createBlocksReader(sliceOutput.slice(), 0);
        Iterator<Block> blockIterator = actualBlocks.iterator();

        assertTrue(blockIterator.hasNext());
        Block block = blockIterator.next();
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
        assertEquals(rleBlock.getSingleValue(), createTuple("alice"));
        assertEquals(rleBlock.getPositionCount(), 2);
        assertEquals(rleBlock.getRange(), Range.create(0, 1));

        assertTrue(blockIterator.hasNext());
        block = blockIterator.next();
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        rleBlock = (RunLengthEncodedBlock) block;
        assertEquals(rleBlock.getSingleValue(), createTuple("bob"));
        assertEquals(rleBlock.getPositionCount(), 4);
        assertEquals(rleBlock.getRange(), Range.create(2, 5));

        assertTrue(blockIterator.hasNext());
        block = blockIterator.next();
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        rleBlock = (RunLengthEncodedBlock) block;
        assertEquals(rleBlock.getSingleValue(), createTuple("charlie"));
        assertEquals(rleBlock.getPositionCount(), 6);
        assertEquals(rleBlock.getRange(), Range.create(6, 11));

        assertFalse(blockIterator.hasNext());
    }
}
