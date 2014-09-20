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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.orc.stream.OrcInputStream;
import com.facebook.presto.hive.orc.stream.StreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.StripeFooter;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.hive.orc.stream.OrcInputStream.BLOCK_HEADER_SIZE;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DICTIONARY;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DICTIONARY_V2;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DIRECT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DIRECT_V2;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.NONE;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DICTIONARY_DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.LENGTH;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.ROW_INDEX;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.SECONDARY;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.StripeInformation;

public class StripeReader
{
    private static final int BYTE_STREAM_POSITIONS = 1;
    private static final int RUN_LENGTH_INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;
    private static final int RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
    private static final int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;

    // for uncompressed streams, what is the most overlap with the following set
    // of rows (long vint literal group).
    private static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;

    private final OrcDataSource orcDataSource;
    private final CompressionKind compressionKind;
    private final List<Type> types;
    private final int bufferSize;
    private final boolean[] includedColumns;
    private final int rowsInRowGroup;
    private final Map<HiveColumnHandle, Integer> columnHandleStreamIndex;
    private final TupleDomain<HiveColumnHandle> tupleDomain;
    private final TypeManager typeManager;

    public StripeReader(OrcDataSource orcDataSource,
            CompressionKind compressionKind,
            List<Type> types,
            int bufferSize,
            boolean[] includedColumns,
            int rowsInRowGroup,
            Map<HiveColumnHandle, Integer> columnHandleStreamIndex,
            TupleDomain<HiveColumnHandle> tupleDomain,
            TypeManager typeManager)
    {
        this.orcDataSource = orcDataSource;
        this.compressionKind = compressionKind;
        this.types = types;
        this.bufferSize = bufferSize;
        this.includedColumns = includedColumns;
        this.rowsInRowGroup = rowsInRowGroup;
        this.columnHandleStreamIndex = columnHandleStreamIndex;
        this.tupleDomain = tupleDomain;
        this.typeManager = typeManager;
    }

    public Stripe readStripe(StripeInformation stripe)
            throws IOException
    {
        // read the stripe footer
        StripeFooter stripeFooter = readStripeFooter(stripe);

        // get streams for selected columns
        Map<StreamId, Stream> streams = new HashMap<>();
        for (Stream stream : stripeFooter.getStreamsList()) {
            if (includedColumns[stream.getColumn()]) {
                streams.put(new StreamId(stream), stream);
            }
        }

        // determine ranges of the stripe to read
        Map<StreamId, DiskRange> diskRanges = getDiskRanges(stripeFooter.getStreamsList());
        diskRanges = Maps.filterKeys(diskRanges, Predicates.in(streams.keySet()));

        // read the file regions
        Map<StreamId, Slice> streamsData = readDiskRanges(stripe.getOffset(), diskRanges);

        // read the row index for each column
        List<RowIndex> indexes = readRowIndex(streams, streamsData);

        // select the row groups matching the tuple domain
        Set<Integer> selectedRowGroups = selectRowGroups(stripe, indexes);

        // if all row groups are skipped, return null
        if (selectedRowGroups.isEmpty()) {
            return null;
        }

        // determine the dictionary stream locations
        List<StreamLayout> dictionaryStreamLayouts = getDictionaryStreams(streams, diskRanges, stripeFooter.getColumnsList());

        // determine the locations of the row groups
        List<RowGroupLayout> rowGroupLayouts = getRowGroupRanges(
                Ints.checkedCast(stripe.getNumberOfRows()),
                streams,
                diskRanges,
                indexes,
                selectedRowGroups,
                stripeFooter.getColumnsList());

        // merge row groups
        rowGroupLayouts = RowGroupLayout.mergeAdjacentRowGroups(rowGroupLayouts);

        // build the dictionary streams
        ImmutableMap.Builder<StreamId, StreamSource<?>> dictionaryStreamBuilder = ImmutableMap.builder();
        for (StreamLayout dictionaryStreamLayout : dictionaryStreamLayouts) {
            StreamId streamId = dictionaryStreamLayout.getStreamId();

            Slice data = streamsData.get(streamId);
            checkArgument(data != null, "No data for stream %s", streamId);

            StreamSource<?> streamSource = dictionaryStreamLayout.createStreamSource(data, bufferSize);
            dictionaryStreamBuilder.put(streamId, streamSource);
        }
        StreamSources dictionaryStreamSources = new StreamSources(dictionaryStreamBuilder.build());

        // build the row groups
        ImmutableList.Builder<RowGroup> rowGroupBuilder = ImmutableList.builder();
        for (RowGroupLayout rowGroupLayout : rowGroupLayouts) {
            rowGroupBuilder.add(rowGroupLayout.createRowGroup(streamsData, bufferSize));
        }

        return new Stripe(stripe.getNumberOfRows(), stripeFooter.getColumnsList(), rowGroupBuilder.build(), dictionaryStreamSources);
    }

    public Map<StreamId, Slice> readDiskRanges(final long stripeOffset, Map<StreamId, DiskRange> diskRanges)
            throws IOException
    {
        // transform ranges to have an absolute offset in file
        diskRanges = Maps.transformValues(diskRanges, new Function<DiskRange, DiskRange>() {
            @Override
            public DiskRange apply(DiskRange diskRange)
            {
                return new DiskRange(stripeOffset + diskRange.getOffset(), diskRange.getLength());
            }
        });
        return orcDataSource.readFully(diskRanges);
    }

    public StripeFooter readStripeFooter(StripeInformation stripe)
            throws IOException
    {
        long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        int tailLength = Ints.checkedCast(stripe.getFooterLength());

        // read the footer
        byte[] tailBuffer = new byte[tailLength];
        orcDataSource.readFully(offset, tailBuffer);
        InputStream inputStream = new OrcInputStream(Slices.wrappedBuffer(tailBuffer).getInput(), compressionKind, bufferSize);
        return StripeFooter.parseFrom(inputStream);
    }

    private List<RowIndex> readRowIndex(Map<StreamId, Stream> streams, Map<StreamId, Slice> streamsData)
            throws IOException
    {
        List<RowIndex> indexes = new ArrayList<>(Collections.<RowIndex>nCopies(types.size(), null));

        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            Stream stream = entry.getValue();
            if (stream.getKind() == ROW_INDEX) {
                Slice slice = streamsData.get(entry.getKey());
                indexes.set(stream.getColumn(), RowIndex.parseFrom(new OrcInputStream(slice.getInput(), compressionKind, bufferSize)));
            }
        }
        return indexes;
    }

    private Set<Integer> selectRowGroups(StripeInformation stripe, List<RowIndex> indexes)
            throws IOException
    {
        int rowsInStripe = Ints.checkedCast(stripe.getNumberOfRows());
        int groupsInStripe = ceil(rowsInStripe, rowsInRowGroup);

        ImmutableSet.Builder<Integer> selectedRowGroups = ImmutableSet.builder();
        int remainingRows = rowsInStripe;
        for (int rowGroup = 0; rowGroup < groupsInStripe; ++rowGroup) {
            int rows = Math.min(remainingRows, rowsInRowGroup);
            TupleDomain<HiveColumnHandle> rowGroupTupleDomain = OrcDomainExtractor.extractDomain(typeManager, columnHandleStreamIndex, indexes, rowGroup, rows);
            if (tupleDomain.overlaps(rowGroupTupleDomain)) {
                selectedRowGroups.add(rowGroup);
            }
            remainingRows -= rows;
        }
        return selectedRowGroups.build();
    }

    private List<StreamLayout> getDictionaryStreams(Map<StreamId, Stream> streams, Map<StreamId, DiskRange> diskRanges, List<ColumnEncoding> encodings)
    {
        // Determine stream ranges for dictionary data
        ImmutableList.Builder<StreamLayout> streamLayouts = ImmutableList.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            StreamId streamId = entry.getKey();
            Stream stream = entry.getValue();
            int column = stream.getColumn();

            // only process dictionary streams
            ColumnEncoding encoding = encodings.get(column);
            if (isIndexStream(stream) || !isDictionary(stream, encoding)) {
                continue;
            }

            StreamLayout streamLayout = new StreamLayout(
                    streamId,
                    0, // dictionary streams don't have a row group
                    types.get(column).getKind(),
                    encoding.getKind(),
                    compressionKind,
                    diskRanges.get(streamId),
                    ImmutableList.<Integer>of());

            streamLayouts.add(streamLayout);
        }

        return streamLayouts.build();
    }

    private List<RowGroupLayout> getRowGroupRanges(
            int rowsInStripe,
            Map<StreamId, Stream> streams,
            Map<StreamId, DiskRange> diskRanges,
            List<RowIndex> indexes,
            Set<Integer> selectedRowGroups,
            List<ColumnEncoding> encodings)
    {
        Set<Integer> columnsWithNulls = new HashSet<>();
        for (Stream stream : streams.values()) {
            if (stream.getKind() == PRESENT) {
                columnsWithNulls.add(stream.getColumn());
            }
        }

        // Determine stream ranges for selected row groups
        ImmutableList.Builder<RowGroupLayout> rowGroupLayouts = ImmutableList.builder();

        for (Integer rowGroupId : selectedRowGroups) {
            ImmutableList.Builder<StreamLayout> streamLayouts = ImmutableList.builder();
            for (Entry<StreamId, Stream> entry : streams.entrySet()) {
                StreamId streamId = entry.getKey();
                Stream stream = entry.getValue();
                int column = stream.getColumn();
                ColumnEncoding encoding = encodings.get(column);

                // only process streams in the data area, that are not dictionaries
                if (isIndexStream(stream) || isDictionary(stream, encoding)) {
                    continue;
                }

                boolean hasNulls = columnsWithNulls.contains(column);
                RowIndex rowIndex = indexes.get(column);
                DiskRange diskRange = getRowGroupStreamDiskRange(rowIndex, diskRanges.get(streamId), hasNulls, rowGroupId, streamId, encoding.getKind());
                List<Long> offsetPositions = getOffsetPositions(streamId, encoding.getKind(), hasNulls, rowIndex.getEntry(rowGroupId).getPositionsList());

                StreamLayout streamLayout = new StreamLayout(
                        streamId,
                        rowGroupId,
                        types.get(column).getKind(),
                        encoding.getKind(),
                        compressionKind,
                        diskRange,
                        checkedCastToInteger(offsetPositions));

                streamLayouts.add(streamLayout);
            }

            int rowsInGroup = Math.min(rowsInStripe - (rowGroupId * rowsInRowGroup), rowsInRowGroup);
            rowGroupLayouts.add(new RowGroupLayout(rowGroupId, rowsInGroup, streamLayouts.build()));
        }

        return rowGroupLayouts.build();
    }

    private DiskRange getRowGroupStreamDiskRange(RowIndex index, DiskRange streamDiskRange, boolean hasNulls, int groupId, StreamId streamId, ColumnEncoding.Kind encoding)
    {
        long start = streamDiskRange.getOffset() + getOffsetPositions(streamId, encoding, hasNulls, index.getEntry(groupId).getPositionsList()).get(0);

        long end;
        if (groupId == index.getEntryCount() - 1) {
            end = streamDiskRange.getEnd();
        }
        else {
            end = streamDiskRange.getOffset() + getOffsetPositions(streamId, encoding, hasNulls, index.getEntry(groupId + 1).getPositionsList()).get(0);

            // for an inner group, we need to add some "slop" to the length
            // since the last value may be in a compressed block or encoded sequence
            // shared with the next row group
            if (compressionKind != NONE) {
                // add 2 buffers to safely accommodate the next compression block.
                end += 2 * (BLOCK_HEADER_SIZE + bufferSize);
            }
            else {
                // add worst case size of an encoded value sequence
                end += WORST_UNCOMPRESSED_SLOP;
            }

            end = Math.min(streamDiskRange.getEnd(), end);
        }

        return new DiskRange(start, Ints.checkedCast(end - start));
    }

    private List<Long> getOffsetPositions(
            StreamId streamId,
            ColumnEncoding.Kind columnEncoding,
            boolean hasNulls,
            List<Long> positionsList)
    {
        Type.Kind type = types.get(streamId.getColumn()).getKind();
        int compressionOffsets = compressionKind != NONE ? 1 : 0;

        // if this is the present stream the offset is in position 1
        List<Long> offsetPositions = positionsList;
        if (streamId.getKind() == PRESENT) {
            return offsetPositions.subList(0, BITFIELD_POSITIONS + compressionOffsets);
        }

        // if this column has nulls, remove offset used by the PRESENT stream
        if (hasNulls) {
            offsetPositions = offsetPositions.subList(BITFIELD_POSITIONS + compressionOffsets, offsetPositions.size());
        }

        if (streamId.getKind() == DATA) {
            switch (type) {
                case BOOLEAN:
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case DATE:
                case STRUCT:
                case MAP:
                case LIST:
                case UNION:
                    // these types should not have any trailing offsets
                    return offsetPositions;
                case STRING:
                case BINARY:
                    if (columnEncoding == DIRECT || columnEncoding == DIRECT_V2) {
                        return offsetPositions.subList(0, BYTE_STREAM_POSITIONS + compressionOffsets);
                    }
                    else if (columnEncoding == DICTIONARY || columnEncoding == DICTIONARY_V2) {
                        return offsetPositions.subList(0, RUN_LENGTH_INT_POSITIONS + compressionOffsets);
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported encoding " + columnEncoding);
                    }
                case TIMESTAMP:
                    return offsetPositions.subList(0, RUN_LENGTH_INT_POSITIONS + compressionOffsets);
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }
        }

        if (streamId.getKind() == LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                    int start = BYTE_STREAM_POSITIONS + compressionOffsets;
                    int end = start + RUN_LENGTH_INT_POSITIONS + compressionOffsets;
                    return offsetPositions.subList(start, end);
                case MAP:
                case LIST:
                    return offsetPositions;
            }
        }

        // length (nanos) of a timestamp column
        if (streamId.getKind() == SECONDARY && type == Type.Kind.TIMESTAMP) {
            int start = RUN_LENGTH_INT_POSITIONS + compressionOffsets;
            int end = start + RUN_LENGTH_INT_POSITIONS + compressionOffsets;
            return offsetPositions.subList(start, end);
        }

        throw new IllegalArgumentException("Unsupported column type " + type + " for stream " + streamId);
    }

    private static boolean isIndexStream(Stream stream)
    {
        return stream.getKind() == ROW_INDEX || stream.getKind() == Kind.DICTIONARY_COUNT;
    }

    private static boolean isDictionary(Stream stream, ColumnEncoding encoding)
    {
        ColumnEncoding.Kind encodingKind = encoding.getKind();
        return stream.getKind() == DICTIONARY_DATA || (stream.getKind() == LENGTH && (encodingKind == DICTIONARY || encodingKind == DICTIONARY_V2));
    }

    private static Map<StreamId, DiskRange> getDiskRanges(List<Stream> streams)
    {
        ImmutableMap.Builder<StreamId, DiskRange> streamDiskRanges = ImmutableMap.builder();
        long stripeOffset = 0;
        for (Stream stream : streams) {
            int streamLength = Ints.checkedCast(stream.getLength());
            streamDiskRanges.put(new StreamId(stream), new DiskRange(stripeOffset, streamLength));
            stripeOffset += streamLength;
        }
        return streamDiskRanges.build();
    }

    /**
     * Ceiling of integer division
     */
    private static int ceil(int dividend, int divisor)
    {
        return ((dividend + divisor) - 1) / divisor;
    }

    private static List<Integer> checkedCastToInteger(List<Long> offsetPositions)
    {
        return ImmutableList.copyOf(Iterables.transform(offsetPositions, new Function<Long, Integer>()
        {
            @Nullable
            @Override
            public Integer apply(Long input)
            {
                return Ints.checkedCast(input);
            }
        }));
    }
}
