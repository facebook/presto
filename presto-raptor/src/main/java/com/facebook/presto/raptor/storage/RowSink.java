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
package com.facebook.presto.raptor.storage;

import java.io.Closeable;

public interface RowSink
        extends Closeable
{
    void beginRecord(long sampleWeight);

    void finishRecord();

    int currentField();

    void appendNull();

    void appendBoolean(boolean value);

    void appendLong(long value);

    void appendDouble(double value);

    void appendString(String value);

    void appendBytes(byte[] value);

    @Override
    void close();
}
