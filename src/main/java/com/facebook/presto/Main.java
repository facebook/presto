/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.ingest.BlockDataImporter;
import com.facebook.presto.ingest.BlockExtractor;
import com.facebook.presto.ingest.DelimitedBlockExtractor;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import io.airlift.command.*;
import io.airlift.command.Cli.CliBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.TupleInfo.Type;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        CliBuilder<Callable<Void>> builder = (CliBuilder<Callable<Void>>) (Object) Cli.buildCli("presto", Callable.class)
                .withDefaultCommand(Help.class)
                .withCommands(Help.class);

        builder.withGroup("convert")
                .withDescription("convert file formats")
                .withDefaultCommand(Help.class)
                .withCommand(ConvertCsv.class);

        Cli<Callable<Void>> cli = builder.build();

        cli.parse(args).call();
    }

    public static class BaseCommand
            implements Callable<Void>
    {
        @Override
        public Void call()
                throws Exception
        {
            run();
            return null;
        }

        public void run()
                throws Exception
        {
            System.out.println(getClass().getSimpleName());
        }
    }

    @Command(name = "csv", description = "Convert CSV to columns")
    public static class ConvertCsv
            extends BaseCommand
    {
        @Option(name = {"-d", "--column-delimiter"}, description = "Column delimiter character")
        public String columnSeparator = ",";

        @Option(name = {"-o", "--output-dir"}, description = "Output directory")
        public String outputDir = "data";

        @Option(name = {"-e", "--extract"}, description = "Column extraction specification")
        public List<String> extractionSpecs;

        @Arguments(description = "CSV file to convert")
        public String csvFile;

        @Override
        public void run()
                throws Exception
        {
            checkArgument(extractionSpecs != null && !extractionSpecs.isEmpty(), "Extraction Spec is required");
            InputSupplier<InputStreamReader> inputSupplier;
            if (csvFile != null) {
                inputSupplier = Files.newReaderSupplier(new File(csvFile), Charsets.UTF_8);
            }
            else {
                inputSupplier = new InputSupplier<InputStreamReader>()
                {
                    public InputStreamReader getInput()
                    {
                        return new InputStreamReader(System.in, Charsets.UTF_8);
                    }
                };
            }

            ImmutableList.Builder<DelimitedBlockExtractor.ColumnDefinition> columnDefinitionBuilder = ImmutableList.builder();
            ImmutableList.Builder<BlockDataImporter.ColumnImportSpec> columnImportSpecBuilder = ImmutableList.builder();
            for (String extractionSpec : extractionSpecs) {
                // Extract column index, base type, and encodingName
                // Examples: '0_long_raw', '3_string_rle', '4_double_dic-rle'
                List<String> parts = ImmutableList.copyOf(Splitter.on('_').split(extractionSpec));
                checkState(parts.size() == 3, "type format: <column_index>_<data_type>_<encoding> (e.g. 0_long_raw, 3_string_rle)");
                Integer columnIndex = Integer.parseInt(parts.get(0));
                String dataTypeName = parts.get(1);
                String encodingName = parts.get(2);

                TupleInfo.Type type;
                switch (dataTypeName) {
                    case "long":
                        type = Type.FIXED_INT_64;
                        break;
                    case "double":
                        type = Type.DOUBLE;
                        break;
                    case "string":
                        type = Type.VARIABLE_BINARY;
                        break;
                    case "fmillis":
                        type = Type.DOUBLE;
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + dataTypeName);
                }

                columnDefinitionBuilder.add(new DelimitedBlockExtractor.ColumnDefinition(columnIndex, type));
                columnImportSpecBuilder.add(
                        new BlockDataImporter.ColumnImportSpec(
                                TupleStreamSerdes.createTupleStreamSerde(encodingName),
                                newOutputStreamSupplier(new File(outputDir, String.format("column%d.%s_%s.data", columnIndex, dataTypeName, encodingName)))
                        )
                );
            }

            BlockExtractor blockExtractor = new DelimitedBlockExtractor(columnDefinitionBuilder.build(), Splitter.on(toChar(columnSeparator)));
            BlockDataImporter importer = new BlockDataImporter(blockExtractor, columnImportSpecBuilder.build());
            importer.importFrom(inputSupplier);
        }

        private char toChar(String string)
        {
            checkArgument(!string.isEmpty(), "String is empty");
            if (string.length() == 1) {
                return string.charAt(0);
            }
            if (string.length() == 6 && string.startsWith("\\u")) {
                int value = Integer.parseInt(string.substring(2), 16);
                return (char) value;
            }
            throw new IllegalArgumentException(String.format("Can not convert '%s' to a char", string));
        }

        private OutputSupplier<FileOutputStream> newOutputStreamSupplier(final File file)
        {
            return new OutputSupplier<FileOutputStream>()
            {
                public FileOutputStream getOutput()
                        throws IOException
                {
                    Files.createParentDirs(file);
                    return new FileOutputStream(file);
                }
            };
        }
    }
}
