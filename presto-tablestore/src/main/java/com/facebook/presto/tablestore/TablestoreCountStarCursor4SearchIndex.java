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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.iterator.SearchRowIterator;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;

import static java.lang.System.currentTimeMillis;

public class TablestoreCountStarCursor4SearchIndex
        implements RecordCursor
{
    protected final long startTimestamp;
    protected final SearchRowIterator iterator;
    protected final long totalRows;
    protected long rows;

    public TablestoreCountStarCursor4SearchIndex(SearchRowIterator iterator)
    {
        this.iterator = iterator;
        this.totalRows = iterator.getTotalCount();
        this.startTimestamp = currentTimeMillis();
    }

    @Override
    public long getReadTimeNanos()
    {
        return (currentTimeMillis() - startTimestamp) * 1000_000;
    }

    @Override
    public long getCompletedBytes()
    {
        return iterator.getCompletedBytes();
    }

    private IllegalArgumentException unsupported()
    {
        return new IllegalArgumentException("Unsupported method call for 'count(*) cursor'");
    }

    @Override
    public Type getType(int field)
    {
        throw unsupported();
    }

    @Override
    public boolean advanceNextPosition()
    {
        return rows++ < totalRows;
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw unsupported();
    }

    @Override
    public long getLong(int field)
    {
        throw unsupported();
    }

    @Override
    public double getDouble(int field)
    {
        throw unsupported();
    }

    @Override
    public Slice getSlice(int field)
    {
        throw unsupported();
    }

    @Override
    public Object getObject(int field)
    {
        throw unsupported();
    }

    @Override
    public boolean isNull(int field)
    {
        throw unsupported();
    }

    @Override
    public void close()
    {
    }
}
