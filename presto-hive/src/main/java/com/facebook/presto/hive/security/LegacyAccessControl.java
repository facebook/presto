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
package com.facebook.presto.hive.security;

import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.Privilege;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static java.util.Objects.requireNonNull;

public class LegacyAccessControl
        implements ConnectorAccessControl
{
    private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;
    private final boolean allowAddColumn;
    private final boolean allowDropColumn;
    private final boolean allowRenameColumn;

    @Inject
    public LegacyAccessControl(
            Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
            LegacySecurityConfig securityConfig)
    {
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");

        requireNonNull(securityConfig, "securityConfig is null");
        allowDropTable = securityConfig.getAllowDropTable();
        allowRenameTable = securityConfig.getAllowRenameTable();
        allowAddColumn = securityConfig.getAllowAddColumn();
        allowDropColumn = securityConfig.getAllowDropColumn();
        allowRenameColumn = securityConfig.getAllowRenameColumn();
    }

    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowDropTable) {
            denyDropTable(tableName.toString());
        }

        Optional<Table> target = metastoreProvider.apply(((HiveTransactionHandle) transaction)).getTable(tableName.getSchemaName(), tableName.getTableName());

        if (!target.isPresent()) {
            denyDropTable(tableName.toString(), "Table not found");
        }

        if (!identity.getUser().equals(target.get().getOwner())) {
            denyDropTable(tableName.toString(), "Owner of the table is different from session user");
        }
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!allowRenameTable) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowAddColumn) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowDropColumn) {
            denyDropColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowRenameColumn) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanSelectFromView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorIdentity identity, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, String revokee, boolean grantOptionFor)
    {
    }
}
