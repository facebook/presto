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
package com.facebook.presto.bigquery;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DateTimeEncoding;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.facebook.presto.bigquery.BigQueryMetadata.NUMERIC_DATA_TYPE_PRECISION;
import static com.facebook.presto.bigquery.BigQueryMetadata.NUMERIC_DATA_TYPE_SCALE;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;

public class BigQueryResultPageSource
        implements ConnectorPageSource
{
    static final AvroDecimalConverter DECIMAL_CONVERTER = new AvroDecimalConverter();
    private static final Logger log = Logger.get(BigQueryResultPageSource.class);
    private final BigQueryStorageClient bigQueryStorageClient;
    private final int maxReadRowsRetries;
    private final BigQuerySplit split;
    private final BigQueryTableHandle table;
    private final ImmutableList<BigQueryColumnHandle> columns;
    private final ImmutableList<Type> columnTypes;
    private final AtomicLong readBytes;
    private final PageBuilder pageBuilder;
    private Iterator<Storage.ReadRowsResponse> responses;
    private boolean closed;

    public BigQueryResultPageSource(
            BigQueryStorageClientFactory bigQueryStorageClientFactory,
            int maxReadRowsRetries,
            BigQuerySplit split,
            BigQueryTableHandle table,
            ImmutableList<BigQueryColumnHandle> columns)
    {
        this.bigQueryStorageClient = bigQueryStorageClientFactory.createBigQueryStorageClient();
        this.maxReadRowsRetries = maxReadRowsRetries;
        this.split = split;
        this.table = table;
        this.columns = columns;
        this.readBytes = new AtomicLong();
        this.columnTypes = columns.stream().map(BigQueryColumnHandle::getPrestoType).collect(toImmutableList());
        this.pageBuilder = new PageBuilder(columnTypes);

        log.debug("Starting to read from %s", split.getStreamName());
        Storage.ReadRowsRequest.Builder readRowsRequest = Storage.ReadRowsRequest.newBuilder()
                .setReadPosition(Storage.StreamPosition.newBuilder()
                        .setStream(Storage.Stream.newBuilder()
                                .setName(split.getStreamName())));
        responses = new ReadRowsHelper(bigQueryStorageClient, readRowsRequest, maxReadRowsRetries).readRows();
        closed = false;
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes.get();
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !responses.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        Storage.ReadRowsResponse response = responses.next();
        Iterable<GenericRecord> records = parse(response);
        for (GenericRecord record : records) {
            pageBuilder.declarePosition();
            for (int column = 0; column < columnTypes.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                appendTo(columnTypes.get(column), record.get(column), output);
            }
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private void appendTo(Type type, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(DATE)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(TIMESTAMP)) {
                    type.writeLong(output, BigQueryType.toPrestoTimestamp(((Utf8) value).toString()));
                }
                else if (type.equals(TIME_WITH_TIME_ZONE)) {
                    type.writeLong(output, DateTimeEncoding.packDateTimeWithZone(((Long) value).longValue() / 1000, TimeZoneKey.UTC_KEY));
                }
                else if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
                    type.writeLong(output, DateTimeEncoding.packDateTimeWithZone(((Long) value).longValue() / 1000, TimeZoneKey.UTC_KEY));
                }
                else {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (javaType == Block.class) {
                writeBlock(output, type, value);
            }
            else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ignore) {
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    private void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof VarcharType) {
            type.writeSlice(output, utf8Slice(((Utf8) value).toString()));
        }
        else if (type instanceof DecimalType) {
            BigDecimal bdValue = DECIMAL_CONVERTER.convert(value);
            type.writeSlice(output, Decimals.encodeScaledValue(bdValue, NUMERIC_DATA_TYPE_SCALE));
        }
        else if (type instanceof VarbinaryType) {
            if (value instanceof ByteBuffer) {
                type.writeSlice(output, Slices.wrappedBuffer((ByteBuffer) value));
            }
            else {
                output.appendNull();
            }
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof ArrayType && value instanceof List<?>) {
            BlockBuilder builder = output.beginBlockEntry();

            for (Object element : (List<?>) value) {
                appendTo(type.getTypeParameters().get(0), element, builder);
            }

            output.closeEntry();
            return;
        }
        if (type instanceof RowType && value instanceof GenericRecord) {
            GenericRecord record = (GenericRecord) value;
            BlockBuilder builder = output.beginBlockEntry();

            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i < type.getTypeSignature().getParameters().size(); i++) {
                TypeSignatureParameter parameter = type.getTypeSignature().getParameters().get(i);
                fieldNames.add(parameter.getNamedTypeSignature().getName().orElse("field" + i));
            }
            checkState(fieldNames.size() == type.getTypeParameters().size(), "fieldName doesn't match with type size : %s", type);
            for (int index = 0; index < type.getTypeParameters().size(); index++) {
                appendTo(type.getTypeParameters().get(index), record.get(fieldNames.get(index)), builder);
            }
            output.closeEntry();
            return;
        }
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        bigQueryStorageClient.close();
        closed = true;
    }

    Iterable<GenericRecord> parse(Storage.ReadRowsResponse response)
    {
        byte[] buffer = response.getAvroRows().getSerializedBinaryRows().toByteArray();
        readBytes.addAndGet(buffer.length);
        log.debug("Read %d bytes (total %d) from %s", buffer.length, readBytes.get(), split.getStreamName());
        Schema avroSchema = new Schema.Parser().parse(split.getAvroSchema());
        return () -> new AvroBinaryIterator(avroSchema, buffer);
    }

    Stream<GenericRecord> toRecords(Storage.ReadRowsResponse response)
    {
        byte[] buffer = response.getAvroRows().getSerializedBinaryRows().toByteArray();
        readBytes.addAndGet(buffer.length);
        log.debug("Read %d bytes (total %d) from %s", buffer.length, readBytes.get(), split.getStreamName());
        Schema avroSchema = new Schema.Parser().parse(split.getAvroSchema());
        Iterable<GenericRecord> responseRecords = () -> new AvroBinaryIterator(avroSchema, buffer);
        return StreamSupport.stream(responseRecords.spliterator(), false);
    }

    static class AvroBinaryIterator
            implements Iterator<GenericRecord>
    {
        GenericDatumReader<GenericRecord> reader;
        BinaryDecoder in;

        AvroBinaryIterator(Schema avroSchema, byte[] buffer)
        {
            this.reader = new GenericDatumReader<>(avroSchema);
            this.in = new DecoderFactory().binaryDecoder(buffer, null);
        }

        @Override
        public boolean hasNext()
        {
            try {
                return !in.isEnd();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error determining the end of Avro buffer", e);
            }
        }

        @Override
        public GenericRecord next()
        {
            try {
                return reader.read(null, in);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading next Avro Record", e);
            }
        }
    }

    static class AvroDecimalConverter
    {
        private static final DecimalConversion AVRO_DECIMAL_CONVERSION = new DecimalConversion();
        private static final Schema AVRO_DECIMAL_SCHEMA = new Schema.Parser().parse(format(
                "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":%d,\"scale\":%d}",
                NUMERIC_DATA_TYPE_PRECISION, NUMERIC_DATA_TYPE_SCALE));

        BigDecimal convert(Object value)
        {
            return AVRO_DECIMAL_CONVERSION.fromBytes((ByteBuffer) value, AVRO_DECIMAL_SCHEMA, AVRO_DECIMAL_SCHEMA.getLogicalType());
        }
    }
}
