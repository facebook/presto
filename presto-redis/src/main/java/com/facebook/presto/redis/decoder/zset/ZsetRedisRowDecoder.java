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
package com.facebook.presto.redis.decoder.zset;

import com.facebook.presto.utils.decoder.FieldDecoder;
import com.facebook.presto.utils.decoder.FieldValueProvider;
import com.facebook.presto.utils.decoder.DecodableColumnHandle;
import com.facebook.presto.utils.decoder.RowDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The row decoder for the 'zset' format. Zset's can contain redis keys for tables
 */
public class ZsetRedisRowDecoder
        implements RowDecoder
{
    public static final String NAME = "zset";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Map<String, String> dataMap, Set<FieldValueProvider> fieldValueProviders, List<DecodableColumnHandle> columnHandles, Map<DecodableColumnHandle, FieldDecoder<?>> fieldDecoders)
    {
        return false;
    }
}
