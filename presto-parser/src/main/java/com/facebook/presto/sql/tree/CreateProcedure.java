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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class CreateProcedure
        extends Statement
{
    private final QualifiedName name;
    private final List<ParameterDeclaration> parameters;
    private final RoutineCharacteristics routineCharacteristics;
    private final Statement statement;

    public CreateProcedure(QualifiedName name,
                          List<ParameterDeclaration> parameters,
                          RoutineCharacteristics routineCharacteristics,
                          Statement statement)
    {
        this.name = checkNotNull(name, "name is null");
        this.parameters = checkNotNull(parameters, "parameters is null");
        this.routineCharacteristics = checkNotNull(routineCharacteristics, "routineCharacteristics is null");
        this.statement = checkNotNull(statement, "statement is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<ParameterDeclaration> getParameters()
    {
        return parameters;
    }

    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    public Statement getStatement()
    {
        return statement;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateProcedure(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameters, routineCharacteristics, statement);
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
        CreateProcedure o = (CreateProcedure) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(parameters, o.parameters) &&
                Objects.equals(routineCharacteristics, o.routineCharacteristics) &&
                Objects.equals(statement, o.statement);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameters", parameters)
                .add("routineCharacteristics", routineCharacteristics)
                .add("statement", statement)
                .toString();
    }
}
