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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.XxHash64;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_STORAGE_ERROR;

public final class StorageUtil
{
    private StorageUtil() {}

    public static long xxhash64(File file)
    {
        try (InputStream in = new FileInputStream(file)) {
            return XxHash64.hash(in);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to read file: " + file, e);
        }
    }
}
