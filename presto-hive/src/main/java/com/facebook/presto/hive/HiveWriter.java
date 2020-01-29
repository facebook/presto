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
package com.facebook.presto.hive;

import com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import com.facebook.presto.hive.PartitionUpdate.UpdateMode;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveWriter
{
    private final HiveFileWriter fileWriter;
    private final Optional<String> partitionName;
    private final UpdateMode updateMode;
    private final FileWriteInfo fileWriteInfo;
    private final String writePath;
    private final String targetPath;
    private final Consumer<HiveWriter> onCommit;
    private final HiveWriterStats hiveWriterStats;

    private long rowCount;
    private long inputSizeInBytes;
    private String fileStats = "";

    public HiveWriter(
            HiveFileWriter fileWriter,
            Optional<String> partitionName,
            UpdateMode updateMode,
            FileWriteInfo fileWriteInfo,
            String writePath,
            String targetPath,
            Consumer<HiveWriter> onCommit,
            HiveWriterStats hiveWriterStats)
    {
        this.fileWriter = requireNonNull(fileWriter, "fileWriter is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.updateMode = requireNonNull(updateMode, "updateMode is null");
        this.fileWriteInfo = requireNonNull(fileWriteInfo, "fileWriteInfo is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.onCommit = requireNonNull(onCommit, "onCommit is null");
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");
    }

    public long getWrittenBytes()
    {
        return fileWriter.getWrittenBytes();
    }

    public long getSystemMemoryUsage()
    {
        return fileWriter.getSystemMemoryUsage();
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public void append(Page dataPage)
    {
        // getRegionSizeInBytes for each row can be expensive; use getRetainedSizeInBytes for estimation
        hiveWriterStats.addInputPageSizesInBytes(dataPage.getRetainedSizeInBytes());
        fileWriter.appendRows(dataPage);
        rowCount += dataPage.getPositionCount();
        inputSizeInBytes += dataPage.getSizeInBytes();
    }

    public void commit()
    {
        List<ColumnStatistics> stats = fileWriter.commit();

        int column = 0;
        for (ColumnStatistics statistics : stats) {
            String min = statistics.getMin();
            String max = statistics.getMax();
            String rows = !statistics.hasNumberOfValues() ? "-1" : String.valueOf(statistics.getNumberOfValues());
            String line = String.format("col %s: {min = %s, max = %s, rows = %s}", column, min, max, rows);
            column++;
            if (column < stats.size()) {
                line += ", ";
            }

            fileStats += line;
        }
        onCommit.accept(this);
    }

    long getValidationCpuNanos()
    {
        return fileWriter.getValidationCpuNanos();
    }

    public Optional<Runnable> getVerificationTask()
    {
        return fileWriter.getVerificationTask();
    }

    public void rollback()
    {
        fileWriter.rollback();
    }

    public PartitionUpdate getPartitionUpdate()
    {
        return new PartitionUpdate(
                partitionName.orElse(""),
                updateMode,
                writePath,
                targetPath,
                ImmutableList.of(new FileWriteInfo(fileWriteInfo.getWriteFileName(), fileWriteInfo.getTargetFileName(), fileStats)),
                rowCount,
                inputSizeInBytes,
                fileWriter.getWrittenBytes());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fileWriter", fileWriter)
                .add("writeFilePath", writePath + "/" + fileWriteInfo.getWriteFileName())
                .add("targetFilePath", targetPath + "/" + fileWriteInfo.getTargetFileName())
                .toString();
    }
}
