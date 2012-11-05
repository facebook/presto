/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.presto.slice;

import com.google.common.base.Objects;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_DOUBLE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_SHORT;

public class BasicSliceOutput extends SliceOutput
{
    private final Slice slice;
    private int size;

    protected BasicSliceOutput(Slice slice)
    {
        this.slice = slice;
    }

    @Override
    public void reset()
    {
        size = 0;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isWritable()
    {
        return writableBytes() > 0;
    }

    @Override
    public int writableBytes()
    {
        return slice.length() - size;
    }

    @Override
    public void writeByte(int value)
    {
        slice.setByte(size, value);
        size += SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int value)
    {
        slice.setShort(size, value);
        size += SIZE_OF_SHORT;
    }

    @Override
    public void writeInt(int value)
    {
        slice.setInt(size, value);
        size += SIZE_OF_INT;
    }

    @Override
    public void writeLong(long value)
    {
        slice.setLong(size, value);
        size += SIZE_OF_LONG;
    }

    @Override
    public void writeDouble(double value)
    {
        slice.setDouble(size, value);
        size += SIZE_OF_DOUBLE;
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        slice.setBytes(size, source, sourceIndex, length);
        size += length;
    }

    @Override
    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
    }

    @Override
    public void writeBytes(Slice source)
    {
        writeBytes(source, 0, source.length());
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length)
    {
        slice.setBytes(size, source, sourceIndex, length);
        size += length;
    }

    @Override
    public void writeBytes(ByteBuffer source)
    {
        int length = source.remaining();
        slice.setBytes(size, source);
        size += length;
    }

    @Override
    public int writeBytes(InputStream in, int length)
            throws IOException
    {
        int writtenBytes = slice.setBytes(size, in, length);
        if (writtenBytes > 0) {
            size += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(ScatteringByteChannel in, int length)
            throws IOException
    {
        int writtenBytes = slice.setBytes(size, in, length);
        if (writtenBytes > 0) {
            size += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(FileChannel in, int position, int length)
            throws IOException
    {
        int writtenBytes = slice.setBytes(size, in, position, length);
        if (writtenBytes > 0) {
            size += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public BasicSliceOutput appendLong(long value)
    {
        writeLong(value);
        return this;
    }

    @Override
    public SliceOutput appendDouble(double value)
    {
        writeDouble(value);
        return this;
    }

    @Override
    public BasicSliceOutput appendInt(int value)
    {
        writeInt(value);
        return this;
    }

    @Override
    public BasicSliceOutput appendShort(int value)
    {
        writeShort(value);
        return this;
    }

    @Override
    public BasicSliceOutput appendBytes(byte[] source, int sourceIndex, int length)
    {
        write(source, sourceIndex, length);
        return this;
    }

    @Override
    public BasicSliceOutput appendBytes(byte[] source)
    {
        writeBytes(source);
        return this;
    }

    @Override
    public BasicSliceOutput appendBytes(Slice slice)
    {
        writeBytes(slice);
        return this;
    }

    @Override
    public Slice slice()
    {
        return slice.slice(0, size);
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        return slice.toByteBuffer(0, size);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("size", size)
                .add("capacity", slice.length())
                .toString();
    }

    public String toString(Charset charset)
    {
        return slice.toString(0, size, charset);
    }
}
