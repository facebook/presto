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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SessionProperty;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetSession;
import com.google.inject.Inject;

import java.util.Set;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SESSION_PROPERTY;
import static java.lang.String.format;

public class SetSessionTask
        implements DataDefinitionTask<SetSession>
{
    @Inject
    ConnectorManager connectorManager;

    @Override
    public String getName()
    {
        return "SET SESSION";
    }

    @Override
    public void execute(SetSession statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        stateMachine.addSetSessionProperties(validateSessionProperty(statement), statement.getValue());
    }

    private String validateSessionProperty(SetSession statement)
    {
        QualifiedName qualifiedName = statement.getName();
        if (qualifiedName.getParts().size() > 2) {
            throw new SemanticException(INVALID_SESSION_PROPERTY, statement, "Invalid session property '%s'", qualifiedName);
        }

        String sessionPropertyName = qualifiedName.toString();
        Set<SessionProperty> availableProperties;

        // if no catalog is specified validate against the system catalog, otherwise
        // validate against "catalog.property" formatted session properties
        // so a system session property 'x' can be specified as either
        // 'x' or 'system.x'
        if (!sessionPropertyName.contains(".")) {
            availableProperties = SystemSessionProperties.getAvailableProperties();
        }
        else {
            availableProperties = connectorManager.getAvailableSessionProperties();
        }

        boolean found = availableProperties.stream().
                map(property -> property.getName()).
                anyMatch(name -> name.equals(sessionPropertyName));

        if (!found) {
            throw new PrestoException(StandardErrorCode.INVALID_SESSION_PROPERTY, format("Invalid session property '%s'", qualifiedName));
        }
        return qualifiedName.toString();
    }
}
