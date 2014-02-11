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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Collections.unmodifiableList;

@Immutable
public class QueryResults
{
    private final QueryStats queryStats;
    private final URI nextUri;
    private final List<Column> columns;
    private final Iterable<List<Object>> data;

    @JsonCreator
    public QueryResults(
            @JsonProperty("queryStats") QueryStats queryStats,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("data") List<List<Object>> data)
    {
        this(queryStats, nextUri, columns, fixData(columns, data));
    }

    public QueryResults(
            QueryStats queryStats,
            URI nextUri,
            List<Column> columns,
            Iterable<List<Object>> data)
    {
        this.queryStats = checkNotNull(queryStats, "queryStats is null");
        this.nextUri = nextUri;
        this.columns = (columns != null) ? ImmutableList.copyOf(columns) : null;
        this.data = (data != null) ? unmodifiableIterable(data) : null;
    }

    @NotNull
    @JsonProperty
    public QueryStats getQueryStats()
    {
        return queryStats;
    }

    @Nullable
    @JsonProperty
    public URI getNextUri()
    {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @Nullable
    @JsonProperty
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("queryStats", queryStats)
                .add("nextUri", nextUri)
                .add("columns", columns)
                .add("hasData", data != null)
                .toString();
    }

    private static Iterable<List<Object>> fixData(List<Column> columns, List<List<Object>> data)
    {
        if (data == null) {
            return null;
        }
        checkNotNull(columns, "columns is null");
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        for (List<Object> row : data) {
            checkArgument(row.size() == columns.size(), "row/column size mismatch");
            List<Object> newRow = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                newRow.add(fixValue(columns.get(i).getType(), row.get(i)));
            }
            rows.add(unmodifiableList(newRow)); // allow nulls in list
        }
        return rows.build();
    }

    /**
     * Force values coming from Jackson to have the expected object type.
     */
    private static Object fixValue(String type, Object value)
    {
        if (value == null) {
            return null;
        }
        switch (type) {
            case "bigint":
                return ((Number) value).longValue();
            case "double":
                if (value instanceof String) {
                    return Double.parseDouble((String) value);
                }
                return ((Number) value).doubleValue();
            case "boolean":
                return Boolean.class.cast(value);
            case "varchar":
                return String.class.cast(value);
        }
        throw new AssertionError("unimplemented type: " + type);
    }
}
