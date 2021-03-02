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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilterImpl;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.slice.Slice;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

@Description("Determines if this element is in the bloom filter")
@ScalarFunction(value = "bloom_filter_contains")
public class InBloomFilterFunction
{
    private static Cache<Integer, BloomFilterForDynamicFilter> myBloomFilterCache = CacheBuilder.newBuilder()
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .initialCapacity(5)
            .maximumSize(10)
            .build();

    private InBloomFilterFunction() {}

    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(@SqlType(StandardTypes.VARCHAR) Slice bloomFilter, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        try {
            BloomFilterForDynamicFilter myBloomFilter = myBloomFilterCache.getIfPresent(bloomFilter.hashCode());
            if (myBloomFilter == null) {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bloomFilter.getBytes());
                InputStreamReader inputStreamReader = new InputStreamReader(byteArrayInputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                ObjectMapper objectMapper = new ObjectMapper();
                myBloomFilter = objectMapper.readValue(bufferedReader.readLine(), BloomFilterForDynamicFilterImpl.class);
                myBloomFilterCache.put(bloomFilter.hashCode(), myBloomFilter);

                return myBloomFilter.contain(new String(value.getBytes()));
            }

            return myBloomFilter.contain(new String(value.getBytes()));
        }
        catch (IOException e) {
            return true;
        }
    }
}
