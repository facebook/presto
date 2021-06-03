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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;

import java.io.IOException;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class IntegerDictionaryProvider
{
    private final InputStreamSources dictionaryStreamSources;
    private final OrcLocalMemoryContext memoryUsage;
    private long totalBytes = 0;

    public IntegerDictionaryProvider(InputStreamSources dictionaryStreamSources, OrcLocalMemoryContext memoryUsage)
    {
        this.dictionaryStreamSources = requireNonNull(dictionaryStreamSources, "dictionaryStreamSources is null");
        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
    }

    /**
     * Loads a dictionary from a stream and attempts to reuse the dictionary buffer passed in.
     *
     * @param streamDescriptor descriptor indicating node and sequence of the stream reader
     * the dictionary is associated with.
     * @param dictionary dictionary buffer the method attempts to fill.
     * @param items number of items expected in the dictionary.
     * @return The final dictionary buffer object. Different from the input dictionary buffer if
     * the input dictionary buffer is expanded.
     * @throws IOException
     */
    public long[] getDictionary(StreamDescriptor streamDescriptor, long[] dictionary, int items)
            throws IOException
    {
        InputStreamSource<LongInputStream> dictionaryDataStream = dictionaryStreamSources.getInputStreamSource(streamDescriptor, DICTIONARY_DATA, LongInputStream.class);
        return loadDictionary(streamDescriptor, requireNonNull(dictionaryDataStream), dictionary, items);
    }

    private long[] loadDictionary(StreamDescriptor streamDescriptor, InputStreamSource<LongInputStream> dictionaryDataStream, long[] dictionaryBuffer, int items)
            throws IOException
    {
        // We construct and use the input stream exactly once per stream descriptor per stripe, so we don't
        // really need to cache it.
        LongInputStream inputStream = dictionaryDataStream.openStream();
        if (inputStream == null) {
            throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Dictionary is not empty but data stream is not present for %s", streamDescriptor);
        }

        if (dictionaryBuffer == null || dictionaryBuffer.length < items) {
            dictionaryBuffer = new long[items];
            totalBytes += sizeOf(dictionaryBuffer);
            memoryUsage.setBytes(totalBytes);
        }

        inputStream.next(dictionaryBuffer, items);
        return dictionaryBuffer;
    }
}
