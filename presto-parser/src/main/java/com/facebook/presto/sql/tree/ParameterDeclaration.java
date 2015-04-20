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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class ParameterDeclaration
        extends Node
{
    public static enum Mode
    {
        IN,
        OUT,
        INOUT
    }

    private final Mode mode;
    private final Optional<String> name;
    private final String type;
    private final Optional<Expression> defaultValue;

    public ParameterDeclaration(Optional<Mode> mode,
                                Optional<String> name,
                                String type,
                                Optional<Expression> defaultValue)
    {
        this.mode = checkNotNull(mode, "mode is null").orElse(Mode.IN);
        this.name = checkNotNull(name, "name is null");
        this.type = checkNotNull(type, "type is null");
        this.defaultValue = checkNotNull(defaultValue, "defaultValue is null");
    }

    public Mode getMode()
    {
        return mode;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public Optional<Expression> getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitParameterDeclaration(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(mode, name, type, defaultValue);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ParameterDeclaration o = (ParameterDeclaration) obj;
        return mode == o.mode &&
                Objects.equals(name, o.name) &&
                Objects.equals(type, o.type) &&
                Objects.equals(defaultValue, o.defaultValue);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("mode", mode)
                .add("name", name)
                .add("type", type)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
