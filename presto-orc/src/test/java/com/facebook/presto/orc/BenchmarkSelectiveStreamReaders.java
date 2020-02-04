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
package com.facebook.presto.orc;

import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import com.facebook.presto.orc.TupleDomainFilter.DoubleRange;
import com.facebook.presto.orc.TupleDomainFilter.FloatRange;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import javafx.util.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnsHive;
import static com.facebook.presto.orc.TupleDomainFilter.LongDecimalRange;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSelectiveStreamReaders
{
    private static final int ROWS = 10_000_000;
    private static final List<?> NULL_VALUES = Collections.nCopies(ROWS, null);
    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(10, 5);
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(30, 10);

    @Benchmark
    public List<Block> readAllBlocks(BenchmarkData data)
            throws IOException
    {
        OrcSelectiveRecordReader recordReader = data.createRecordReader();

        List<Block> blocks = new ArrayList<>();
        while (true) {
            Page page = recordReader.getNextPage();
            if (page == null) {
                break;
            }

            if (page.getPositionCount() > 0) {
                for (int i = 0; i < page.getChannelCount(); i++) {
                    blocks.add(page.getBlock(i).getLoadedBlock());
                }
            }
        }

        return blocks;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int NO_FILTER = -1;

        private final Random random = new Random(0);

        private File temporaryDirectory;
        private File orcFile;
        private Type type;
        private int channelCount;
        private int nonEmptyFilterCount;
        private List<Optional<TupleDomainFilter>> filters = new ArrayList<>();
        private List<Float> filterRates = new ArrayList<>();

        @Param({
                "boolean",

                "integer",
                "bigint",
                "smallint",
                "tinyint",

                "date",
                "timestamp",

                "real",
                "double",
                "decimal(10,5)",
                "decimal(30,10)",

                "varchar_direct",
                "varchar_dictionary"
        })
        private String typeSignature = "boolean";

        @Param({
                "PARTIAL",
                "NONE",
                "ALL"
        })
        private Nulls withNulls = Nulls.PARTIAL;

        // 0 means no rows will be filtered out, 1 means all rows will be filtered out, -1 means no filter.
        // When withNulls is ALL, only -1, 0, 1 are meaningful. Other values are regarded as 1.
        // "|" is the column delimiter.
        @Param({
                "-1",
                "0",
                "0.1",
                "0.5",
                "0.9",
                "1",
                "0.0|-1",
                "0.1|-1",
                "0.2|-1",
                "0.3|-1",
                "0.4|-1",
                "0.5|-1",
                "0.6|-1",
                "0.8|-1",
                "0.9|-1",
                "1|-1",
        })
        private String filterRateSignature = "0.1|-1";

        @Setup
        public void setup()
                throws Exception
        {
            if (typeSignature.startsWith("varchar")) {
                type = new TypeRegistry().getType(TypeSignature.parseTypeSignature("varchar"));
            }
            else {
                type = new TypeRegistry().getType(TypeSignature.parseTypeSignature(typeSignature));
            }

            temporaryDirectory = createTempDir();
            orcFile = new File(temporaryDirectory, randomUUID().toString());

            filterRates = Arrays.stream(filterRateSignature.split("\\|")).map(r -> Float.parseFloat(r)).collect(toImmutableList());
            channelCount = filterRates.size();

            List<List<?>> values = new ArrayList<>();
            for (int i = 0; i < channelCount; i++) {
                float filterRate = filterRates.get(i);
                Pair<Boolean, Float> filterInfoForNonNull = getFilterInfoForNonNull(filterRate);
                values.add(createValues(type, filterRate));
                Optional<TupleDomainFilter> filter = getFilter(type, filterRate, filterInfoForNonNull.getKey(), filterInfoForNonNull.getValue());
                filters.add(filter);
                if (filter.isPresent()) {
                    nonEmptyFilterCount++;
                }
            }

            writeOrcColumnsHive(orcFile, ORC_12, NONE, Collections.nCopies(channelCount, type), values);
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        public OrcSelectiveRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(orcFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(
                    dataSource,
                    ORC,
                    new StorageOrcFileTailSource(),
                    new StorageStripeMetadataSource(),
                    OrcReaderTestingUtils.createDefaultTestConfig());

            return orcReader.createSelectiveRecordReader(
                    IntStream.range(0, channelCount).boxed().collect(Collectors.toMap(Function.identity(), i -> type)),
                    IntStream.range(0, channelCount).boxed().collect(Collectors.toList()),
                    nonEmptyFilterCount > 0 ?
                            IntStream.range(0, channelCount).filter(i -> filters.get(i).isPresent()).boxed().collect(Collectors.toMap(Function.identity(), i -> ImmutableMap.of(new Subfield("c"), filters.get(i).orElse(null))))
                            : ImmutableMap.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    OrcPredicate.TRUE,
                    0,
                    dataSource.getSize(),
                    UTC, // arbitrary
                    true,
                    newSimpleAggregatedMemoryContext(),
                    Optional.empty(),
                    INITIAL_BATCH_SIZE);
        }

        private Optional<TupleDomainFilter> getFilter(Type type, float filterRate, boolean filterAllowNull, float selectionRateForNonNull)
        {
            if (filterRate == NO_FILTER) {
                return Optional.empty();
            }

            if (type == BOOLEAN) {
                return Optional.of(BooleanValue.of(true, filterAllowNull));
            }

            if (type == BIGINT) {
                return Optional.of(BigintRange.of((long) (Long.MIN_VALUE * selectionRateForNonNull), (long) (Long.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == INTEGER || type == DATE || type == TIMESTAMP) {
                return Optional.of(BigintRange.of((long) (Integer.MIN_VALUE * selectionRateForNonNull), (long) (Integer.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == INTEGER) {
                return Optional.of(BigintRange.of((long) (Integer.MIN_VALUE * selectionRateForNonNull), (long) (Integer.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == SMALLINT) {
                return Optional.of(BigintRange.of((long) (Short.MIN_VALUE * selectionRateForNonNull), (long) (Short.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == TINYINT) {
                return Optional.of(BigintRange.of((long) (Byte.MIN_VALUE * selectionRateForNonNull), (long) (Byte.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == REAL) {
                return Optional.of(FloatRange.of(0, false, false, selectionRateForNonNull, false, true, filterAllowNull));
            }

            if (type == DOUBLE) {
                return Optional.of(DoubleRange.of(0, false, false, selectionRateForNonNull, false, true, filterAllowNull));
            }

            if (type instanceof DecimalType) {
                if (((DecimalType) type).isShort()) {
                    return Optional.of(BigintRange.of((long) (-10_000_000_000L * selectionRateForNonNull), (long) (10_000_000_000L * selectionRateForNonNull), filterAllowNull));
                }
                return Optional.of(LongDecimalRange.of(
                        (long) (-10_000_000_000L * selectionRateForNonNull),
                        (long) (-10_000_000_000L * selectionRateForNonNull),
                        false,
                        true,
                        (long) (10_000_000_000L * selectionRateForNonNull),
                        (long) (10_000_000_000L * selectionRateForNonNull),
                        false,
                        true,
                        filterAllowNull));
            }

            if (type instanceof VarcharType) {
                if (typeSignature.equals("varchar_dictionary")) {
                    return Optional.of(BytesRange.of("000000000".getBytes(), false, "000000000".getBytes(), filterRate == 1 ? true : false, filterAllowNull));
                }

                return Optional.of(BytesRange.of("000000000".getBytes(), false, String.format("%09d", (int) (999_999_999 * selectionRateForNonNull) - 1).getBytes(), filterRate == 1 ? true : false, filterAllowNull));
            }

            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        private List<?> createValues(Type type, float filterRate)
        {
            switch (withNulls) {
                case ALL:
                    return NULL_VALUES;
                case PARTIAL:
                    // Let the null rate be 0.5 * (1 - filterRate)
                    return IntStream.range(0, ROWS).mapToObj(j -> random.nextFloat() > 0.5 * (filterRate == -1 ? 1 : 1 - filterRate) ? createValue(type, filterRate) : null).collect(toList());
                default:
                    return IntStream.range(0, ROWS).mapToObj(j -> createValue(type, filterRate)).collect(toList());
            }
        }

        private final Object createValue(Type type, float filterRate)
        {
            if (type == BOOLEAN) {
                // We need to specialize BOOLEAN case because we can't specify filterRate by manipulating the filter value in getFilter.
                // Since the filters allows null, so all nulls would all be selected. To make the total selected positions equal to ( 1- filterRate) * positionCount,
                // we need to adapt the filterRate for non null values as follows:
                return random.nextFloat() <= (1 - filterRate) / (1 + filterRate);
            }

            if (type == BIGINT) {
                return random.nextLong();
            }

            if (type == INTEGER) {
                return random.nextInt();
            }

            if (type == SMALLINT) {
                return (short) random.nextInt();
            }

            if (type == TINYINT) {
                return (byte) random.nextInt();
            }

            if (type == DATE) {
                return new SqlDate(random.nextInt());
            }

            if (type == TIMESTAMP) {
                // We use int because longs will be converted to int when being written.
                long value = random.nextInt();
                return new SqlTimestamp(value, TimeZoneKey.UTC_KEY);
            }

            if (type == REAL) {
                return random.nextFloat();
            }

            if (type == DOUBLE) {
                return random.nextDouble();
            }

            if (type instanceof DecimalType) {
                if (Decimals.isShortDecimal(type)) {
                    return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), SHORT_DECIMAL_TYPE.getPrecision(), SHORT_DECIMAL_TYPE.getScale());
                }
                else {
                    return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), LONG_DECIMAL_TYPE.getPrecision(), LONG_DECIMAL_TYPE.getScale());
                }
            }

            if (type == VARCHAR) {
                if (typeSignature.equals("varchar_dictionary")) {
                    return Strings.repeat("0", 9);
                }

                return randomAsciiString(random);
            }

            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        private Pair<Boolean, Float> getFilterInfoForNonNull(float filterRate)
        {
            switch (withNulls) {
                case NONE:
                    return new Pair<>(false, 1 - filterRate);
                case PARTIAL:
                    return new Pair<>(true, (1 - filterRate) / (1 + filterRate));
                case ALL:
                    return new Pair<>((filterRate == 0 ? true : false), 1f);
                default:
                    throw new UnsupportedOperationException("Unsupported withNulls: " + withNulls);
            }
        }

        public enum Nulls
        {
            PARTIAL, NONE, ALL;
        }
    }

    private static String randomAsciiString(Random random)
    {
        return String.format("%09d", random.nextInt(999_999_999));
    }

    public static void main(String[] args)
            throws Throwable
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkSelectiveStreamReaders benchmarkSelectiveStreamReaders = new BenchmarkSelectiveStreamReaders();
        benchmarkSelectiveStreamReaders.readAllBlocks(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSelectiveStreamReaders.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
