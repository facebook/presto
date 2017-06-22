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
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final int MSSQL_VARCHAR_MAX = 8000;
    private static final int MSSQL_CHAR_MAX = 8000;

    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new SQLServerDriver());
        connectionProperties.setProperty("applicationName", "Presto");
        connectionProperties.setProperty("sendStringParametersAsUnicode", "false");
    }

    @Override
    public Connection getConnection(JdbcSplit split)
            throws SQLException
    {
        Connection connection = super.getConnection(split);
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET QUOTED_IDENTIFIER, ANSI_NULLS, CONCAT_NULL_YIELDS_NULL, ARITHABORT ON");
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle)
    {
        try (Connection connection = getConnection(handle);
                PreparedStatement statement = connection.prepareStatement("EXEC sp_rename ?, ?")) {
            statement.setString(1, quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()));
            statement.setString(2, handle.getTableName());
            statement.executeUpdate();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String toSqlType(Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded() || varcharType.getLengthSafe() > MSSQL_VARCHAR_MAX) {
                return "varchar(max)";
            }
            return "varchar(" + ((VarcharType) type).getLengthSafe() + ')';
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            if (charType.getLength() > MSSQL_CHAR_MAX) {
                return "char(max)";
            }
            return "char(" + charType.getLength() + ')';
        }

        return super.toSqlType(type);
    }

    @Override
    protected String quoted(String name)
    {
        return '[' + name.replace("]", "]]") + ']';
    }
}
