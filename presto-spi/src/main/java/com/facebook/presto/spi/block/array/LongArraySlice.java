/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.block.array;

import io.airlift.slice.Slice;

public class LongArraySlice
        implements LongArray
{
    private final int length;
    private final Slice slice;
    private final long retainedSize;

    public LongArraySlice(Slice slice)
    {
        this.length = slice.length() / 8;
        this.slice = slice;
        retainedSize = slice.getRetainedSize();
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public long get(int position)
    {
        return slice.getLong(position * 8);
    }

    @Override
    public long getRetainedSize()
    {
        return retainedSize;
    }

    @Override
    public void set(int position, long value)
    {
        slice.setLong(position * 8, value);
    }
}
