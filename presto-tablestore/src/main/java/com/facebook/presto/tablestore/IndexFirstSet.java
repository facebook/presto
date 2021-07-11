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
package com.facebook.presto.tablestore;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.tablestore.TablestoreSessionProperties.MAX_VALUE_OF_INDEX_FIRST_MAX_PERCENT;
import static com.facebook.presto.tablestore.TablestoreSessionProperties.MAX_VALUE_OF_INDEX_FIRST_MAX_ROWS;
import static com.google.common.base.Preconditions.checkState;

public class IndexFirstSet
{
    private static final String INDEX_HINT_PARSE_ERROR_MESSAGE = "use hint like these: " +
            "1)use index if possible -> 'tablestore-index-first=auto' " +
            "2)do not use any index, default -> 'tablestore-index-first=none' " +
            "3)use indexes of tables that specified -> 'tablestore-index-first=[xxDb.yyTable, zzTable, ...]' " +
            "4)use heuristic rule of max matched rows -> 'tablestore-index-first=threshold:1000' " +
            "5)use heuristic rule of max matched percentage -> 'tablestore-index-first=threshold:5%'";

    enum Type
    {
        AUTO("auto"),
        NONE("none"),
        CUSTOM("custom"),
        THRESHOLD("threshold");

        private String code;

        Type(String code)
        {
            this.code = code;
        }

        public String getCode()
        {
            return code;
        }
    }

    private final Type type;
    private int maxPercent = -1;
    private int maxRows = -1;
    private Set<SchemaTableName> tables = Collections.emptySet();

    private static final IndexFirstSet NONE;
    private static final IndexFirstSet AUTO;

    static {
        NONE = new IndexFirstSet(Type.NONE);
        AUTO = new IndexFirstSet(Type.AUTO);
    }

    private IndexFirstSet(Type type)
    {
        this.type = type;
    }

    /**
     * Do not use index
     */
    static IndexFirstSet none()
    {
        return NONE;
    }

    /**
     * Use index if applicable.
     */
    static IndexFirstSet auto()
    {
        return AUTO;
    }

    public Set<SchemaTableName> getTables()
    {
        if (type == Type.CUSTOM || type == Type.NONE) {
            return tables;
        }
        throw new IllegalStateException("Can't enumerate all the tables for '" + type.getCode() + "' type");
    }

    public boolean isMaxRowsMode()
    {
        return type == Type.THRESHOLD && maxRows > 0;
    }

    public int getMaxPercent()
    {
        if (type == Type.THRESHOLD && maxPercent > 0) {
            return maxPercent;
        }
        throw new IllegalStateException("Can't get max percent for '" + type.getCode() + "' type");
    }

    public int getMaxRows()
    {
        if (type == Type.THRESHOLD && maxRows > 0) {
            return maxRows;
        }
        throw new IllegalStateException("Can't get max rows for '" + type.getCode() + "' type");
    }

    /**
     * Manually config which index to use.
     */
    static IndexFirstSet custom(@Nonnull Set<SchemaTableName> tables)
    {
        if (tables.size() == 0) {
            return none();
        }
        else {
            IndexFirstSet x = new IndexFirstSet(Type.CUSTOM);
            x.tables = ImmutableSet.copyOf(tables);
            return x;
        }
    }

    static IndexFirstSet thresholdWithPercent(int maxPercent)
    {
        IndexFirstSet x = new IndexFirstSet(Type.THRESHOLD);
        x.maxPercent = maxPercent;
        return x;
    }

    static IndexFirstSet thresholdWithRows(int maxRows)
    {
        IndexFirstSet x = new IndexFirstSet(Type.THRESHOLD);
        x.maxRows = maxRows;
        return x;
    }

    public boolean isContained(@Nonnull SchemaTableName table)
    {
        if (this == AUTO || this.type == Type.THRESHOLD) {
            return true;
        }
        if (this == NONE) {
            return false;
        }
        return tables.contains(table);
    }

    public String backToString()
    {
        if (type == Type.AUTO || type == Type.NONE) {
            return type.getCode();
        }
        if (type == Type.THRESHOLD) {
            String x = type.getCode();
            x += maxPercent > 0 ? ":" + maxPercent + "%" : "";
            x += maxRows > 0 ? ":" + maxRows : "";
            return x;
        }
        Optional<String> x = tables.stream().map(SchemaTableName::toString).reduce((a, b) -> a + "," + b);
        return "[" + x.orElse("") + "]";
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static IndexFirstSet parseSearchIndexFirst(Optional<String> currentConnectionSchema, String hintKey, String hintValue)
    {
        if (StringUtils.isBlank(hintValue)) {
            return IndexFirstSet.none();
        }
        String hint = hintValue.trim().toLowerCase(Locale.ENGLISH);
        if (Type.AUTO.getCode().equals(hint)) {
            return IndexFirstSet.auto();
        }
        if (Type.NONE.getCode().equals(hint)) {
            return IndexFirstSet.none();
        }
        String pattern = "^" + Type.THRESHOLD.getCode() + ":\\s*(\\d+)\\s*(%)?$";
        if (hint.matches(pattern)) {
            String err = "Invalid 'threshold' hint value '" + hintValue + "' of hint key '" + hintKey + "', regular pattern='" + pattern + "'";
            Matcher m = Pattern.compile(pattern).matcher(hint);
            checkState(m.find(), err);

            int integer = Integer.parseInt(m.group(1));
            String percent = m.group(2);

            if (percent != null) {
                if (integer < 1 || integer > MAX_VALUE_OF_INDEX_FIRST_MAX_PERCENT) {
                    err = "Invalid 'threshold:${maxPercent}%' hint value '" + hintValue + "' of hint key '" + hintKey + "', "
                            + "which[" + integer + "] should be within the range [1, " + MAX_VALUE_OF_INDEX_FIRST_MAX_PERCENT + "]";
                    throw new IllegalArgumentException(err);
                }
                return thresholdWithPercent(integer);
            }
            else {
                if (integer < 1 || integer > MAX_VALUE_OF_INDEX_FIRST_MAX_ROWS) {
                    err = "Invalid 'threshold:${maxRows}' hint value '" + hintValue + "' of hint key '" + hintKey + "', "
                            + "which[" + integer + "] should be within the range [1, " + MAX_VALUE_OF_INDEX_FIRST_MAX_ROWS + "]";
                    throw new IllegalArgumentException(err);
                }
                return thresholdWithRows(integer);
            }
        }
        if (hint.matches("^\\[[^\\[\\]]*]$")) {
            hint = hint.substring(1, hint.length() - 1).trim();
            if (StringUtils.isBlank(hint)) {
                return IndexFirstSet.none();
            }
            String[] ss = hint.split("[ ;,]+");
            Set<SchemaTableName> tables = Arrays.stream(ss)
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .map(x -> checkAndAssemblySchemaTableName(currentConnectionSchema, hintKey, x))
                    .collect(Collectors.toSet());
            return IndexFirstSet.custom(tables);
        }

        throw new IllegalArgumentException("Invalid hint value '" + hint + "' of hint key '" + hintKey + "', " + INDEX_HINT_PARSE_ERROR_MESSAGE);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static SchemaTableName checkAndAssemblySchemaTableName(Optional<String> currentConnectionSchema,
            String hintKey,
            String unit)
    {
        String s;
        if (!unit.contains(".")) { //'schema'.'table'
            s = currentConnectionSchema.<IllegalArgumentException>orElseThrow(() -> {
                String str = "Can't obtain the schema of the table[" + unit + "] from current connection for hint '"
                        + hintKey + "'";
                throw new IllegalArgumentException(str);
            }) + "." + unit;
        }
        else {
            s = unit;
        }

        String[] parts = s.split("\\.");
        return new SchemaTableName(parts[0], parts[1]);
    }

    public Type getType()
    {
        return type;
    }
}
