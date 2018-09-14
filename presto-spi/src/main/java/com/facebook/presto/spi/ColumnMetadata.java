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
package com.facebook.presto.spi;

import com.facebook.presto.spi.type.Type;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Locale.ENGLISH;

public class ColumnMetadata
{
    private final String name;
    private final Type type;
    private final String comment;
    private final String extraInfo;
    private final boolean hidden;
    private final Map<String, Object> properties;
    private final Object defaultValue;
    private final boolean nullable;

    public ColumnMetadata(String name, Type type)
    {
        this(name, type, null, null, false, emptyMap(), null, true);
    }

    public ColumnMetadata(String name, Type type, String comment, boolean hidden)
    {
        this(name, type, comment, null, hidden, emptyMap(), null, true);
    }

    public ColumnMetadata(String name, Type type, String comment, String extraInfo, boolean hidden)
    {
        this(name, type, comment, extraInfo, hidden, emptyMap(), null, true);
    }

    public ColumnMetadata(String name, Type type, String comment, String extraInfo, boolean hidden, Map<String, Object> properties)
    {
        this(name, type, comment, extraInfo, hidden, properties, null, true);
    }

    public ColumnMetadata(String name, Type type, String comment, String extraInfo, boolean hidden, Map<String, Object> properties, Object defaultValue, boolean nullable)
    {
        if (name == null || name.isEmpty()) {
            throw new NullPointerException("name is null or empty");
        }
        if (type == null) {
            throw new NullPointerException("type is null");
        }
        if (properties == null) {
            throw new NullPointerException("properties is null");
        }

        this.name = name.toLowerCase(ENGLISH);
        this.type = type;
        this.comment = comment;
        this.extraInfo = extraInfo;
        this.hidden = hidden;
        this.properties = properties.isEmpty() ? emptyMap() : unmodifiableMap(new LinkedHashMap<>(properties));
        this.defaultValue = defaultValue;
        this.nullable = nullable;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public String getComment()
    {
        return comment;
    }

    public String getExtraInfo()
    {
        return extraInfo;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public Map<String, Object> getProperties()
    {
        return properties;
    }

    public Object getDefaultValue()
    {
        return defaultValue;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ColumnMetadata{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        if (comment != null) {
            sb.append(", comment='").append(comment).append('\'');
        }
        if (extraInfo != null) {
            sb.append(", extraInfo='").append(extraInfo).append('\'');
        }
        if (hidden) {
            sb.append(", hidden");
        }
        if (!properties.isEmpty()) {
            sb.append(", properties=").append(properties);
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment, extraInfo, hidden);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnMetadata other = (ColumnMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.extraInfo, other.extraInfo) &&
                Objects.equals(this.hidden, other.hidden);
    }
}
