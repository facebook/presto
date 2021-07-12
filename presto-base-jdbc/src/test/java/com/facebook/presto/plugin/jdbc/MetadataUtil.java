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
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

final class MetadataUtil
{
    private MetadataUtil() {}

    public static final JsonCodec<JdbcColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<JdbcTableHandle> TABLE_CODEC;
    public static final JsonCodec<JdbcOutputTableHandle> OUTPUT_TABLE_CODEC;

    static {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        COLUMN_CODEC = codecFactory.jsonCodec(JdbcColumnHandle.class);
        TABLE_CODEC = codecFactory.jsonCodec(JdbcTableHandle.class);
        OUTPUT_TABLE_CODEC = codecFactory.jsonCodec(JdbcOutputTableHandle.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<StandardTypes.Types, Type> types = ImmutableMap.of(
                StandardTypes.Types.BIGINT, BIGINT,
                StandardTypes.Types.VARCHAR, VARCHAR);

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            StandardTypes.Types standardType = StandardTypes.Types.getTypeFromString(value.toLowerCase(ENGLISH));
            checkArgument(standardType != null, "%s type is not supported.", value);
            Type type = types.get(standardType);
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }

    public static <T> void assertJsonRoundTrip(JsonCodec<T> codec, T object)
    {
        String json = codec.toJson(object);
        T copy = codec.fromJson(json);
        assertEquals(copy, object);
    }
}
