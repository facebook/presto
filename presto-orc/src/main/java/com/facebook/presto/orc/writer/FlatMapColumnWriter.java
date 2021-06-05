
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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.MetadataWriter;

import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder;
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.stream.LongOutputStream.createLengthOutputStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;

public class FlatMapColumnWriter implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FlatMapColumnWriter.class).instanceSize();

    /* Column Ids for the the Map, Key and Value */
    private final int column;
    private final int sequence;
    private final int keyColumnId;
    private final int valueColumnId;
    /* Type data for for the Map */
    private final MapType type;
    private final Type keyType;
    private final Type valueType;
    /* We need the OrcType of the key to perform Type specific Block writes */
    private final OrcType keyOrcType;
    private final boolean compressed;
    /* Column Encoding data for the Map */
    private final ColumnEncoding columnEncoding;
    private final ColumnEncoding keyColumnEncoding;
    private final ColumnEncoding valueColumnEncoding;
    private final Optional<DwrfDataEncryptor> dwrfEncryptor;
    /* Metadata writer for the Map type and copy of the passed metadata writer to pass to FlatMapKeyNode writer */
    private final CompressedMetadataWriter metadataWriter;
    private final MetadataWriter inputMetadataWriter;
    /* valueWriterFactory to generate streams for individual keys */
    private final ColumnWriterFactory valueWriterFactory;
    private final int stringStatisticsLimitInBytes;

    private final LongOutputStream lengthStream;
    private final PresentOutputStream presentStream;
    private List<FlatMapKeyNodeWriter> keyNodes;
    /* Block Writer for Numeric or String Keyed Types */
    private final FlatMapBlockWriter blockWriter;
    private StatisticsBuilder statisticsBuilder;
    private final Supplier<StatisticsBuilder> statisticsBuilderSupplier;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private final List<ColumnStatistics> keyRowGroupColumnStatistics = new ArrayList<>();
    private long columnStatisticsRetainedSizeInBytes;

    private int nonNullValueCount;
    private boolean closed;

    private StringStatisticsBuilder newStringStatisticsBuilder()
    {
        return new StringStatisticsBuilder(stringStatisticsLimitInBytes);
    }

    public FlatMapColumnWriter(
            int column,
            int sequence,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            MetadataWriter metadataWriter,
            Type type,
            List<OrcType> orcTypes,
            ColumnWriterFactory valueWriterfactory)
    {
        checkArgument(column >= 0, "column is negative");
        checkArgument(sequence >= 0, "sequence is negative");

        this.column = column;
        this.sequence = sequence;
        this.keyColumnId = orcTypes.get(column).getFieldTypeIndex(0);
        this.valueColumnId = orcTypes.get(column).getFieldTypeIndex(1);

        this.type = (MapType) type;
        this.keyType = type.getTypeParameters().get(0);
        this.valueType = type.getTypeParameters().get(1);

        this.keyOrcType = orcTypes.get(keyColumnId);

        this.compressed = columnWriterOptions.getCompressionKind() != NONE;

        this.columnEncoding = new ColumnEncoding(orcEncoding == DWRF ? DWRF_MAP_FLAT : DIRECT_V2, 0);
        this.keyColumnEncoding = new ColumnEncoding(orcEncoding == DWRF ? DIRECT : DIRECT_V2, 0);
        this.valueColumnEncoding = new ColumnEncoding(orcEncoding == DWRF ? DIRECT : DIRECT_V2, 0);

        this.dwrfEncryptor = dwrfEncryptor;
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
        this.inputMetadataWriter = metadataWriter;

        this.valueWriterFactory = valueWriterfactory;
        this.stringStatisticsLimitInBytes = toIntExact(columnWriterOptions.getStringStatisticsLimit().toBytes());


        this.lengthStream = createLengthOutputStream(columnWriterOptions, dwrfEncryptor, orcEncoding);
        this.presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);
        this.keyNodes = new ArrayList<>();

        switch (keyOrcType.getOrcTypeKind()) {
            case CHAR:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                // fall through
            case VARCHAR:
            case STRING:
                this.blockWriter = new FlatMapStringKeyBlockWriter(this.valueColumnId, columnWriterOptions, dwrfEncryptor, inputMetadataWriter, type, this.keyOrcType, this.valueWriterFactory, this.keyNodes);
                this.statisticsBuilderSupplier = this::newStringStatisticsBuilder;
                break;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                this.blockWriter = new FlatMapNumericKeyBlockWriter(this.valueColumnId, columnWriterOptions, dwrfEncryptor, inputMetadataWriter, type, this.keyOrcType, this.valueWriterFactory, this.keyNodes);
                this.statisticsBuilderSupplier = IntegerStatisticsBuilder::new;
                break;
            default:
                throw new IllegalArgumentException("Unsupported type for FlatMap Keys : " + type);
        }
        this.statisticsBuilder = statisticsBuilderSupplier.get();
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        return this.blockWriter.getNestedColumnWriters();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<Integer, ColumnEncoding> encodings = ImmutableMap.builder();
        encodings.put(column, columnEncoding);
        encodings.put(keyColumnId, keyColumnEncoding);
        encodings.putAll(this.blockWriter.getColumnEncodings());
        return encodings.build();
    }

    @Override
    public void beginRowGroup()
    {
        lengthStream.recordCheckpoint();
        presentStream.recordCheckpoint();
        this.blockWriter.beginRowGroup();
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        ColumnarMap columnarMap = toColumnarMap(block);
        // write nulls and lengths
        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            boolean present = !columnarMap.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                nonNullValueCount++;
                lengthStream.writeLong(columnarMap.getEntryCount(position));
            }
        }
        this.blockWriter.writeColumnarMap(columnarMap);
        this.statisticsBuilder.addBlock(keyType, columnarMap.getKeysBlock());
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);

        /* Map Type column statistics is just the nonNullValueCount */
        ColumnStatistics statistics = new ColumnStatistics((long) nonNullValueCount, 0, null, null, null, null, null, null, null, null);
        rowGroupColumnStatistics.add(statistics);
        columnStatisticsRetainedSizeInBytes += statistics.getRetainedSizeInBytes();
        nonNullValueCount = 0;

        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        /* Add statistics for map type */
        columnStatistics.put(column, statistics);

        /* Add statistics for the key Type */
        ColumnStatistics keyStatistics = statisticsBuilder.buildColumnStatistics();
        keyRowGroupColumnStatistics.add(keyStatistics);
        columnStatisticsRetainedSizeInBytes += keyStatistics.getRetainedSizeInBytes();
        columnStatistics.put(keyColumnId, keyStatistics);
        statisticsBuilder = statisticsBuilderSupplier.get();

        /* Add statistics for the value Type */
        columnStatistics.put(valueColumnId,
                ColumnStatistics.mergeColumnStatistics(
                        this.blockWriter.finishRowGroup().entrySet().stream().
                                map(entry -> entry.getValue()).
                                collect(toList())));
        return columnStatistics.build();
    }

    @Override
    public void close()
    {
        closed = true;
        this.blockWriter.close();
        lengthStream.close();
        presentStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        /* Add stats for map type */
        columnStatistics.put(column, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
        /* add stats for key type */
        columnStatistics.put(keyColumnId, ColumnStatistics.mergeColumnStatistics(keyRowGroupColumnStatistics));
        /* add stats for value type */
        columnStatistics.put(valueColumnId,
                ColumnStatistics.mergeColumnStatistics(
                        this.blockWriter.getColumnStripeStatistics().entrySet().stream().
                                map(entry -> entry.getValue()).
                                collect(toList())));
        return columnStatistics.build();
    }

    /**
     * Write index streams to the output and return the streams in the
     * order in which they were written.  The ordering is critical because
     * the stream only contain a length with no offset.
     */
    @Override
    public List<StreamDataOutput> getIndexStreams()
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        /* Create the ROW INDEX Stream for the MAP type */
        List<LongStreamCheckpoint> lengthCheckpoints = lengthStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            LongStreamCheckpoint lengthCheckpoint = lengthCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createArrayColumnPositionList(compressed, lengthCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }
        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(column, Stream.StreamKind.ROW_INDEX, slice.length(), false, sequence, Optional.empty());
        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.add(new StreamDataOutput(slice, stream));

        /* There are no ROW_INDEX streams for key type in Flat Maps
         * Instead each sequence in value type has a ROW_INDEX stream  */
        indexStreams.addAll(this.blockWriter.getIndexStreams());
        return indexStreams.build();
    }

    private static List<Integer> createArrayColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint lengthCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(lengthCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    /**
     * Get the data streams to be written.
     */
    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);
        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        outputDataStreams.addAll(this.blockWriter.getDataStreams());
        /* The Map type only records the PRESENT Stream  as a DATA Stream */
        presentStream.getStreamDataOutput(column).ifPresent(outputDataStreams::add);
        return outputDataStreams.build();
    }

    /**
     * This method returns the size of the flushed data plus any unflushed data.
     * If the output is compressed, flush data size is the size after compression.
     */
    @Override
    public long getBufferedBytes()
    {
        return lengthStream.getBufferedBytes() +
                presentStream.getBufferedBytes() +
                this.blockWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                lengthStream.getRetainedBytes() +
                presentStream.getRetainedBytes() +
                this.blockWriter.getRetainedBytes() +
                columnStatisticsRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        lengthStream.reset();
        presentStream.reset();
        this.blockWriter.reset();
        rowGroupColumnStatistics.clear();
        columnStatisticsRetainedSizeInBytes = 0;
        nonNullValueCount = 0;
        statisticsBuilder = statisticsBuilderSupplier.get();
    }
}
