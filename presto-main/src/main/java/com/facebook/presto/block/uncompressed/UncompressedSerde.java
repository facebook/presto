/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamDeserializer;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.TupleStreamSerializer;
import com.facebook.presto.block.TupleStreamWriter;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.slice.ByteArraySlice;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.slice.Slices;
import com.google.common.collect.AbstractIterator;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.block.Cursors.advanceNextPositionNoYield;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedSerde
        implements TupleStreamSerde
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();
    public static final UncompressedSerde INSTANCE = new UncompressedSerde();

    @Override
    public TupleStreamSerializer createSerializer()
    {
        return new TupleStreamSerializer() {
            @Override
            public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
            {
                return new UncompressedTupleStreamWriter(sliceOutput);
            }
        };
    }

    @Override
    public TupleStreamDeserializer createDeserializer()
    {
        return new TupleStreamDeserializer() {
            @Override
            public TupleStream deserialize(Range totalRange, Slice slice)
            {
                return readAsStream(totalRange, slice);
            }

            @Override
            public Blocks deserializeBlocks(Range totalRange, Slice slice)
            {
                return com.facebook.presto.nblock.uncompressed.UncompressedSerde.deserializeBlocks(totalRange, slice);
            }
        };
    }

    private static void write(SliceOutput destination, long startPosition, int tupleCount, Slice slice)
    {
        ByteArraySlice header = Slices.allocate(SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_LONG);
        header.getOutput()
                .appendInt(slice.length())
                .appendInt(tupleCount)
                .appendLong(startPosition);
        destination.writeBytes(header);
        destination.writeBytes(slice);
    }

    public static TupleStream read(File file)
            throws IOException
    {
        Slice mappedSlice = Slices.mapFileReadOnly(file);
        return INSTANCE.createDeserializer().deserialize(Range.ALL, mappedSlice);
    }

    public static Iterator<UncompressedBlock> read(Slice slice)
    {
        return new UncompressedReader(0, slice);
    }

    public static TupleStream readAsStream(final Range totalRange, final Slice slice)
    {
        TupleInfo tupleInfo = new UncompressedReader(0, slice).tupleInfo;

        return new GenericTupleStream<>(tupleInfo, new Iterable<UncompressedBlock>()
        {
            @Override
            public Iterator<UncompressedBlock> iterator()
            {
                return new UncompressedReader(totalRange.getStart(), slice);
            }
        });
    }

    private static class UncompressedTupleStreamWriter
            implements TupleStreamWriter
    {
        private final SliceOutput sliceOutput;

        private boolean initialized;
        private boolean finished;
        private long currentStartPosition = -1;
        private DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);
        private int tupleCount;

        private UncompressedTupleStreamWriter(SliceOutput sliceOutput)
        {
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
        }

        @Override
        public TupleStreamWriter append(TupleStream tupleStream)
        {
            checkNotNull(tupleStream, "tupleStream is null");
            checkState(!finished, "already finished");

            if (!initialized) {
                // todo We should be able to take advantage of the fact that the cursor might already be over uncompressed blocks and just write them down as they come.
                UncompressedTupleInfoSerde.serialize(tupleStream.getTupleInfo(), sliceOutput);
                initialized = true;
            }

            Cursor cursor = tupleStream.cursor(new QuerySession());

            while (advanceNextPositionNoYield(cursor)) {
                if (currentStartPosition == -1) {
                    currentStartPosition = cursor.getPosition();
                }
                cursor.getTuple().writeTo(buffer);
                tupleCount++;

                if (buffer.size() >= MAX_BLOCK_SIZE) {
                    write(sliceOutput, currentStartPosition, tupleCount, buffer.slice());
                    tupleCount = 0;
                    buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);
                    currentStartPosition = -1;
                }
            }

            return this;
        }

        @Override
        public void finish()
        {
            checkState(initialized, "nothing appended");
            checkState(!finished, "already finished");
            finished = true;

            if (buffer.size() > 0) {
                checkState(currentStartPosition >= 0, "invariant");
                write(sliceOutput, currentStartPosition, tupleCount, buffer.slice());
            }
        }
    }

    private static class UncompressedReader
            extends AbstractIterator<UncompressedBlock>
    {
        private final long positionOffset;
        private final TupleInfo tupleInfo;
        private final SliceInput sliceInput;

        private UncompressedReader(long positionOffset, Slice slice)
        {
            this.positionOffset = positionOffset;
            sliceInput = slice.getInput();
            this.tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);
        }

        protected UncompressedBlock computeNext()
        {
            if (!sliceInput.isReadable()) {
                endOfData();
                return null;
            }

            int blockSize = sliceInput.readInt();
            int tupleCount = sliceInput.readInt();
            long startPosition = sliceInput.readLong() + positionOffset;

            Range range = Range.create(startPosition, startPosition + tupleCount - 1);

            Slice block = sliceInput.readSlice(blockSize);
            return new UncompressedBlock(range, tupleInfo, block);
        }
    }
}
