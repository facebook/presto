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
package com.facebook.presto.sql.relational;

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Map;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;

public class ProjectNodeUtils
{
    private ProjectNodeUtils() {}

    public static boolean isIdentity(ProjectNode projectNode)
    {
        for (Map.Entry<Symbol, RowExpression> entry : projectNode.getAssignments().entrySet()) {
            RowExpression value = entry.getValue();
            Symbol symbol = entry.getKey();
            if (isExpression(value)) {
                Expression expression = castToExpression(value);
                if (!(expression instanceof SymbolReference && ((SymbolReference) expression).getName().equals(symbol.getName()))) {
                    return false;
                }
            }
            else {
                if (!(value instanceof VariableReferenceExpression && ((VariableReferenceExpression) value).getName().equals(symbol.getName()))) {
                    return false;
                }
            }
        }
        return true;
    }
}
