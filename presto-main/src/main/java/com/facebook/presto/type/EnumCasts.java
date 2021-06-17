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

package com.facebook.presto.type;

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.function.ScalarOperator;
import com.facebook.presto.common.function.SqlType;
import com.facebook.presto.common.function.TypeParameter;
import com.facebook.presto.common.type.BigintEnumType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharEnumType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;

public final class EnumCasts
{
    private EnumCasts()
    {
    }

    @ScalarOperator(CAST)
    @TypeParameter(value = "T", boundedBy = VarcharEnumType.class)
    @SqlType("T")
    public static Slice castVarcharToEnum(@TypeParameter("T") Type enumType, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (!(((VarcharEnumType) enumType).getEnumMap().values().contains(value.toStringUtf8()))) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    String.format(
                            "No value '%s' in enum '%s'",
                            value.toStringUtf8(),
                            enumType.getTypeSignature().getBase()));
        }
        return value;
    }

    @ScalarOperator(CAST)
    @TypeParameter(value = "T", boundedBy = VarcharEnumType.class)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castEnumToVarchar(@SqlType("T") Slice value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @TypeParameter(value = "T", boundedBy = BigintEnumType.class)
    @SqlType("T")
    public static long castBigintToEnum(@TypeParameter("T") Type enumType, @SqlType(BIGINT) long value)
    {
        return castLongToEnum(enumType, value);
    }

    @ScalarOperator(CAST)
    @TypeParameter(value = "T", boundedBy = BigintEnumType.class)
    @SqlType("T")
    public static long castIntegerToEnum(@TypeParameter("T") Type enumType, @SqlType(StandardTypes.INTEGER) long value)
    {
        return castLongToEnum(enumType, value);
    }

    @ScalarOperator(CAST)
    @TypeParameter(value = "T", boundedBy = BigintEnumType.class)
    @SqlType("T")
    public static long castSmallintToEnum(@TypeParameter("T") Type enumType, @SqlType(StandardTypes.SMALLINT) long value)
    {
        return castLongToEnum(enumType, value);
    }

    @ScalarOperator(CAST)
    @TypeParameter(value = "T", boundedBy = BigintEnumType.class)
    @SqlType("T")
    public static long castTinyintToEnum(@TypeParameter("T") Type enumType, @SqlType(StandardTypes.TINYINT) long value)
    {
        return castLongToEnum(enumType, value);
    }

    private static long castLongToEnum(Type enumType, long value)
    {
        if (!((BigintEnumType) enumType).getEnumMap().values().contains(value)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    String.format(
                            "No value '%d' in enum '%s'",
                            value,
                            enumType.getTypeSignature().getBase()));
        }
        return value;
    }

    @ScalarOperator(CAST)
    @TypeParameter(value = "T", boundedBy = BigintEnumType.class)
    @SqlType(BIGINT)
    public static long castEnumToBigint(@SqlType("T") long value)
    {
        return value;
    }
}
