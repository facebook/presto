/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedBlockSerde
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();

    private UncompressedBlockSerde()
    {
    }

    public static void write(Iterator<ValueBlock> iterator, File file)
            throws IOException
    {
        try (FileOutputStream out = new FileOutputStream(file)) {
            write(iterator, out);
        }
    }

    public static void write(Iterator<ValueBlock> iterator, OutputStream out)
            throws IOException
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(MAX_BLOCK_SIZE);

        int tupleCount = 0;
        TupleInfo tupleInfo = null;
        while (iterator.hasNext()) {
            ValueBlock valueBlock = iterator.next();
            for (Tuple tuple : valueBlock) {
                if (tupleInfo == null) {
                    // write the tuple info
                    tupleInfo = tuple.getTupleInfo();
                    writeTupleInfo(out, tupleInfo);
                }
                else {
                    Preconditions.checkState(tupleInfo == tuple.getTupleInfo(), "Expected %s, but got %s", tupleInfo, tuple.getTupleInfo());
                }
                tuple.writeTo(sliceOutput);
                tupleCount++;

                if (sliceOutput.size() > MAX_BLOCK_SIZE) {
                    write(out, tupleCount, sliceOutput);
                    tupleCount = 0;
                    sliceOutput = new DynamicSliceOutput(MAX_BLOCK_SIZE);
                }
            }
        }
        if (sliceOutput.size() > 0) {
            write(out, tupleCount, sliceOutput);
        }
    }

    private static void writeTupleInfo(OutputStream out, TupleInfo tupleInfo)
            throws IOException
    {
        out.write(tupleInfo.getFieldCount());
        for (Type type : tupleInfo.getTypes()) {
            out.write(type.ordinal());
        }
    }

    private static void write(OutputStream out, int tupleCount, DynamicSliceOutput sliceOutput)
            throws IOException
    {
        ByteArraySlice header = Slices.allocate(SIZE_OF_INT + SIZE_OF_INT);
        header.output()
                .appendInt(sliceOutput.size())
                .appendInt(tupleCount);
        header.getBytes(0, out, header.length());

        Slice slice = sliceOutput.slice();
        slice.getBytes(0, out, slice.length());
    }

    public static Iterator<ValueBlock> read(File file)
            throws IOException
    {
        Slice mappedSlice = Slices.mapFileReadOnly(file);
        return read(mappedSlice);
    }

    public static Iterator<ValueBlock> read(Slice slice)
    {
        return new UncompressedReader(slice);
    }

    private static class UncompressedReader extends AbstractIterator<ValueBlock>
    {
        private final TupleInfo tupleInfo;
        private final SliceInput sliceInput;
        private int position;

        private UncompressedReader(Slice slice)
        {
            sliceInput = slice.input();

            int fieldCount = sliceInput.readUnsignedByte();
            Builder<Type> builder = ImmutableList.builder();
            for (int i = 0; i < fieldCount; i++) {
                builder.add(Type.values()[sliceInput.readUnsignedByte()]);
            }
            this.tupleInfo = new TupleInfo(builder.build());
        }

        protected ValueBlock computeNext()
        {
            if (!sliceInput.isReadable()) {
                endOfData();
                return null;
            }

            int blockSize = sliceInput.readInt();
            int tupleCount = sliceInput.readInt();

            Range<Long> range = Ranges.closed((long) position, (long) position + tupleCount - 1);
            position += tupleCount;

            Slice block = sliceInput.readSlice(blockSize);
            return new UncompressedValueBlock(range, tupleInfo, block);
        }
    }
}
