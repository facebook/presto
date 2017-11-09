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
package com.facebook.presto.tests.datatype;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Strings.padEnd;
import static java.util.Optional.empty;

public class DataType<T>
{
    private String insertType;
    private Type prestoResultType;
    private Function<T, String> toLiteral;
    private Function<T, ?> toPrestoQueryResult;

    public static DataType<Long> bigintDataType()
    {
        return dataType("bigint", BigintType.BIGINT);
    }

    public static DataType<Integer> integerDataType()
    {
        return dataType("integer", IntegerType.INTEGER);
    }

    public static DataType<Short> smallintDataType()
    {
        return dataType("smallint", SmallintType.SMALLINT);
    }

    public static DataType<Byte> tinyintDataType()
    {
        return dataType("tinyint", TinyintType.TINYINT);
    }

    public static DataType<Double> doubleDataType()
    {
        return dataType("double", DoubleType.DOUBLE);
    }

    public static DataType<Float> realDataType()
    {
        return dataType("real", RealType.REAL);
    }

    public static DataType<String> varcharDataType(int size)
    {
        return varcharDataType(size, "");
    }

    public static DataType<String> varcharDataType(int size, String properties)
    {
        return varcharDataType(Optional.of(size), properties);
    }

    public static DataType<String> varcharDataType()
    {
        return varcharDataType(empty(), "");
    }

    private static DataType<String> varcharDataType(Optional<Integer> length, String properties)
    {
        String prefix = length.map(size -> "varchar(" + size + ")").orElse("varchar");
        String suffix = properties.isEmpty() ? "" : " " + properties;
        VarcharType varcharType = length.map(VarcharType::createVarcharType).orElse(createUnboundedVarcharType());
        return stringDataType(prefix + suffix, varcharType);
    }

    public static DataType<String> stringDataType(String insertType, Type prestoResultType)
    {
        return dataType(insertType, prestoResultType, DataType::quote, Function.identity());
    }

    public static DataType<String> charDataType(int length)
    {
        return charDataType(length, "");
    }

    public static DataType<String> charDataType(int length, String properties)
    {
        String suffix = properties.isEmpty() ? "" : " " + properties;
        return charDataType("char(" + length + ")" + suffix, length);
    }

    public static DataType<String> charDataType(String insertType, int length)
    {
        return dataType(insertType, createCharType(length), DataType::quote, input -> padEnd(input, length, ' '));
    }

    public static String quote(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }

    public static String quotedString(Object value)
    {
        return quote(String.valueOf(value));
    }

    public static <T> DataType<T> dataType(String insertType, Type prestoResultType)
    {
        return new DataType<>(insertType, prestoResultType, Object::toString, Function.identity());
    }

    public static <T> DataType<T> dataType(String insertType, Type prestoResultType, Function<T, String> toLiteral, Function<T, ?> toPrestoQueryResult)
    {
        return new DataType<>(insertType, prestoResultType, toLiteral, toPrestoQueryResult);
    }

    private DataType(String insertType, Type prestoResultType, Function<T, String> toLiteral, Function<T, ?> toPrestoQueryResult)
    {
        this.insertType = insertType;
        this.prestoResultType = prestoResultType;
        this.toLiteral = toLiteral;
        this.toPrestoQueryResult = toPrestoQueryResult;
    }

    public String toLiteral(T inputValue)
    {
        return toLiteral.apply(inputValue);
    }

    public Object toPrestoQueryResult(T inputValue)
    {
        return toPrestoQueryResult.apply(inputValue);
    }

    public String getInsertType()
    {
        return insertType;
    }

    public Type getPrestoResultType()
    {
        return prestoResultType;
    }
}
