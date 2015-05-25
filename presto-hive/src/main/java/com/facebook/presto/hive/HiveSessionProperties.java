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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import io.airlift.units.DataSize;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public final class HiveSessionProperties
{
    public static final String STORAGE_CLASS_NAME_PROPERTY = "storage_class";
    public static final String STORAGE_FORMAT_PROPERTY = "storage_format";
    private static final String FORCE_LOCAL_SCHEDULING = "force_local_scheduling";
    private static final String OPTIMIZED_READER_ENABLED = "optimized_reader_enabled";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_BUFFER_SIZE = "orc_max_buffer_size";
    private static final String ORC_STREAM_BUFFER_SIZE = "orc_stream_buffer_size";

    private HiveSessionProperties()
    {
    }

    public static HiveStorageFormat getHiveStorageFormat(ConnectorSession session, PredefinedHiveStorageFormat defaultValue)
    {
        String storageClassNameString = session.getProperties().get(STORAGE_CLASS_NAME_PROPERTY);
        String storageFormatString = session.getProperties().get(STORAGE_FORMAT_PROPERTY);
        if (storageFormatString == null && storageClassNameString == null) {
            return defaultValue;
        }

        if (storageClassNameString != null) {
            if (storageFormatString != null) {
                throw new PrestoException(INVALID_SESSION_PROPERTY, "Hive storage format and storage class defined simultaneously");
            }
            try {
                return CustomHiveStorageFormat.valueOf(storageClassNameString);
            }
            catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new PrestoException(INVALID_SESSION_PROPERTY, "Hive storage-class is invalid: " + storageClassNameString, e);
            }
        }

        try {
            return PredefinedHiveStorageFormat.valueOf(storageFormatString.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Hive storage-format is invalid: " + storageFormatString, e);
        }
    }

    public static boolean isOptimizedReaderEnabled(ConnectorSession session, boolean defaultValue)
    {
        return isEnabled(OPTIMIZED_READER_ENABLED, session, defaultValue);
    }

    public static DataSize getOrcMaxMergeDistance(ConnectorSession session, DataSize defaultValue)
    {
        String maxMergeDistanceString = session.getProperties().get(ORC_MAX_MERGE_DISTANCE);
        if (maxMergeDistanceString == null) {
            return defaultValue;
        }

        try {
            return DataSize.valueOf(maxMergeDistanceString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, ORC_MAX_MERGE_DISTANCE + " is invalid: " + maxMergeDistanceString);
        }
    }

    public static DataSize getOrcMaxBufferSize(ConnectorSession session, DataSize defaultValue)
    {
        String maxBufferSizeString = session.getProperties().get(ORC_MAX_BUFFER_SIZE);
        if (maxBufferSizeString == null) {
            return defaultValue;
        }

        try {
            return DataSize.valueOf(maxBufferSizeString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, ORC_MAX_BUFFER_SIZE + " is invalid: " + maxBufferSizeString);
        }
    }

    public static DataSize getOrcStreamBufferSize(ConnectorSession session, DataSize defaultValue)
    {
        String streamBufferSizeString = session.getProperties().get(ORC_STREAM_BUFFER_SIZE);
        if (streamBufferSizeString == null) {
            return defaultValue;
        }

        try {
            return DataSize.valueOf(streamBufferSizeString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, ORC_STREAM_BUFFER_SIZE + " is invalid: " + streamBufferSizeString);
        }
    }

    private static boolean isEnabled(String propertyName, ConnectorSession session, boolean defaultValue)
    {
        String enabled = session.getProperties().get(propertyName);
        if (enabled == null) {
            return defaultValue;
        }

        return Boolean.valueOf(enabled);
    }

    public static boolean getForceLocalScheduling(ConnectorSession session, boolean defaultValue)
    {
        String forceLocalScheduling = session.getProperties().get(FORCE_LOCAL_SCHEDULING);
        if (forceLocalScheduling == null) {
            return defaultValue;
        }

        try {
            return Boolean.valueOf(forceLocalScheduling);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NOT_SUPPORTED, "Invalid Hive session property '" + FORCE_LOCAL_SCHEDULING + "=" + forceLocalScheduling + "'");
        }
    }
}
