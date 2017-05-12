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
package com.facebook.presto.sql.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TimeLiteral
        extends Literal
{
    private final String value;

    public TimeLiteral(String value)
    {
        this(Optional.empty(), value);
    }

    public TimeLiteral(NodeLocation location, String value)
    {
        this(Optional.of(location), value);
    }

    private TimeLiteral(Optional<NodeLocation> location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public <C, R> R accept(AstVisitor<C, R> visitor, C context)
    {
        return visitor.visitTimeLiteral(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeLiteral that = (TimeLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
