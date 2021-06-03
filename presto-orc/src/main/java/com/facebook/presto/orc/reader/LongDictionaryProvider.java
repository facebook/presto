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
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LongDictionaryProvider
{
    private final InputStreamSources dictionaryStreamSources;
    private final Map<Integer, DictionaryEntry> sharedDictionaries;

    public static class DictionaryResult
    {
        private final long[] dictionaryBuffer;
        // Can be false if not the first to load shared dictionary.
        private final boolean isBufferOwner;

        DictionaryResult(long[] dictionaryBuffer, boolean isBufferOwner)
        {
            this.dictionaryBuffer = requireNonNull(dictionaryBuffer);
            this.isBufferOwner = isBufferOwner;
        }

        public long[] dictionaryBuffer()
        {
            return dictionaryBuffer;
        }

        public boolean isBufferOwner()
        {
            return isBufferOwner;
        }
    }

    class DictionaryEntry
    {
        DictionaryEntry(long[] values, int size)
        {
            this.values = values;
            this.size = size;
        }

        public final long[] values;
        public final int size;
    }

    public LongDictionaryProvider(InputStreamSources dictionaryStreamSources)
    {
        this.dictionaryStreamSources = requireNonNull(dictionaryStreamSources, "dictionaryStreamSources is null");
        this.sharedDictionaries = new HashMap<>();
    }

    /**
     * Loads a dictionary from a stream and attempts to reuse the dictionary buffer passed in.
     *
     * @param streamDescriptor descriptor indicating node and sequence of the stream reader
     * the dictionary is associated with.
     * @param dictionary dictionary buffer the method attempts to fill.
     * @param items number of items expected in the dictionary.
     * @return The DictionaryResult contains two parts:
     * 1) the final dictionary buffer object. Different from the input dictionary buffer if the input
     *    dictionary buffer is expanded or that the method returns a shared dictionary.
     * 2) whether the caller will be the owner of the dictionary, for the purpose of memory accounting.
     *    Callers own all non-shared dictionaries, and only the first caller of the shared dictionary
     *    is the owner.
     * @throws IOException
     */
    public DictionaryResult getDictionary(StreamDescriptor streamDescriptor, long[] dictionary, int items)
            throws IOException
    {
        InputStreamSource<LongInputStream> dictionaryDataStream = dictionaryStreamSources.getInputStreamSource(streamDescriptor, DICTIONARY_DATA, LongInputStream.class);

        if (streamDescriptor.getSequence() == 0 || requireNonNull(dictionaryDataStream).openStream() != null) {
            return requireNonNull(loadDictionary(streamDescriptor, dictionaryDataStream, dictionary, items));
        }

        // Try fetching shared dictionaries
        DictionaryEntry entry = sharedDictionaries.get(streamDescriptor.getStreamId());
        boolean isNewEntry = entry == null;
        if (isNewEntry) {
            StreamDescriptor sharedDictionaryStreamDescriptor = streamDescriptor.dupWithSequence(0);
            InputStreamSource<LongInputStream> sharedDictionaryDataStream = dictionaryStreamSources.getInputStreamSource(sharedDictionaryStreamDescriptor, DICTIONARY_DATA, LongInputStream.class);
            entry = new DictionaryEntry(loadDictionary(streamDescriptor, sharedDictionaryDataStream, dictionary, items).dictionaryBuffer(), items);
            sharedDictionaries.put(sharedDictionaryStreamDescriptor.getStreamId(), entry);
        }

        checkArgument(entry.size == items, String.format("Shared dictionary size mismatch for stream: %s", streamDescriptor));
        return new DictionaryResult(entry.values, isNewEntry);
    }

    private DictionaryResult loadDictionary(StreamDescriptor streamDescriptor, InputStreamSource<LongInputStream> dictionaryDataStream, long[] dictionaryBuffer, int items)
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
        }

        inputStream.next(dictionaryBuffer, items);
        return new DictionaryResult(dictionaryBuffer, true);
    }
}
