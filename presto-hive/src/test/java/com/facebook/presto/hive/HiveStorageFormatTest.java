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

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class HiveStorageFormatTest
{
    private final JsonCodec<HiveStorageFormat> codec = JsonCodec.jsonCodec(HiveStorageFormat.class);

    @Test
    public void testPredefinedStorageFormatRoundTrip()
    {
        HiveStorageFormat expected = PredefinedHiveStorageFormat.ORC;

        String json = codec.toJson(expected);
        HiveStorageFormat actual = codec.fromJson(json);

        assertEquals(actual.getSerDe(), expected.getSerDe());
        assertEquals(actual.getInputFormat(), expected.getInputFormat());
        assertEquals(actual.getOutputFormat(), expected.getOutputFormat());
    }

    @Test
    public void testCustomStorageFormatRoundTrip()
    {
        String serde = "ala";
        String inputFormat = "has";
        String outputFormat = "a cat";
        HiveStorageFormat expected = new CustomHiveStorageFormat(serde, inputFormat, outputFormat);

        String json = codec.toJson(expected);
        HiveStorageFormat actual = codec.fromJson(json);

        assertEquals(actual.getSerDe(), serde);
        assertEquals(actual.getInputFormat(), inputFormat);
        assertEquals(actual.getOutputFormat(), outputFormat);
    }
}
