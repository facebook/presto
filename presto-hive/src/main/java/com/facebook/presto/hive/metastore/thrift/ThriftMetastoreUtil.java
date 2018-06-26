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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.security.SelectedRole;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class ThriftMetastoreUtil
{
    private ThriftMetastoreUtil() {}

    public static org.apache.hadoop.hive.metastore.api.Database toMetastoreApiDatabase(Database database)
    {
        org.apache.hadoop.hive.metastore.api.Database result = new org.apache.hadoop.hive.metastore.api.Database();
        result.setName(database.getDatabaseName());
        database.getLocation().ifPresent(result::setLocationUri);
        result.setOwnerName(database.getOwnerName());
        result.setOwnerType(toMetastoreApiPrincipalType(database.getOwnerType()));
        database.getComment().ifPresent(result::setDescription);
        result.setParameters(database.getParameters());
        return result;
    }

    public static org.apache.hadoop.hive.metastore.api.Table toMetastoreApiTable(Table table, PrincipalPrivileges privileges)
    {
        org.apache.hadoop.hive.metastore.api.Table result = new org.apache.hadoop.hive.metastore.api.Table();
        result.setDbName(table.getDatabaseName());
        result.setTableName(table.getTableName());
        result.setOwner(table.getOwner());
        result.setTableType(table.getTableType());
        result.setParameters(table.getParameters());
        result.setPartitionKeys(table.getPartitionColumns().stream().map(ThriftMetastoreUtil::toMetastoreApiFieldSchema).collect(toList()));
        result.setSd(makeStorageDescriptor(table.getTableName(), table.getDataColumns(), table.getStorage()));
        result.setPrivileges(toMetastoreApiPrincipalPrivilegeSet(new PrestoPrincipal(USER, table.getOwner()), privileges));
        result.setViewOriginalText(table.getViewOriginalText().orElse(null));
        result.setViewExpandedText(table.getViewExpandedText().orElse(null));
        return result;
    }

    private static PrincipalPrivilegeSet toMetastoreApiPrincipalPrivilegeSet(PrestoPrincipal grantee, PrincipalPrivileges privileges)
    {
        ImmutableMap.Builder<String, List<PrivilegeGrantInfo>> userPrivileges = ImmutableMap.builder();
        for (Map.Entry<String, Collection<HivePrivilegeInfo>> entry : privileges.getUserPrivileges().asMap().entrySet()) {
            userPrivileges.put(entry.getKey(), entry.getValue().stream()
                    .map(privilegeInfo -> toMetastoreApiPrivilegeGrantInfo(grantee, privilegeInfo))
                    .collect(toList()));
        }

        ImmutableMap.Builder<String, List<PrivilegeGrantInfo>> rolePrivileges = ImmutableMap.builder();
        for (Map.Entry<String, Collection<HivePrivilegeInfo>> entry : privileges.getRolePrivileges().asMap().entrySet()) {
            rolePrivileges.put(entry.getKey(), entry.getValue().stream()
                    .map(privilegeInfo -> toMetastoreApiPrivilegeGrantInfo(grantee, privilegeInfo))
                    .collect(toList()));
        }

        return new PrincipalPrivilegeSet(userPrivileges.build(), ImmutableMap.of(), rolePrivileges.build());
    }

    public static PrivilegeGrantInfo toMetastoreApiPrivilegeGrantInfo(PrestoPrincipal grantee, HivePrivilegeInfo privilegeInfo)
    {
        return new PrivilegeGrantInfo(
                privilegeInfo.getHivePrivilege().name().toLowerCase(),
                0,
                grantee.getName(),
                fromPrestoPrincipalType(grantee.getType()),
                privilegeInfo.isGrantOption());
    }

    public static org.apache.hadoop.hive.metastore.api.PrincipalType toMetastoreApiPrincipalType(PrincipalType principalType)
    {
        switch (principalType) {
            case USER:
                return org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
            case ROLE:
                return org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
            default:
                throw new IllegalArgumentException("Unsupported principal type: " + principalType);
        }
    }

    public static Set<RoleGrant> listApplicableRoles(PrestoPrincipal principal, Function<PrestoPrincipal, Set<RoleGrant>> listRoleGrants)
    {
        Set<RoleGrant> result = new HashSet<>();
        Queue<PrestoPrincipal> queue = new ArrayDeque<>();
        queue.add(principal);
        while (!queue.isEmpty()) {
            PrestoPrincipal current = queue.poll();
            Set<RoleGrant> grants = listRoleGrants.apply(current);
            for (RoleGrant grant : grants) {
                if (!result.contains(grant)) {
                    result.add(grant);
                    queue.add(new PrestoPrincipal(ROLE, grant.getRoleName()));
                }
            }
        }
        return ImmutableSet.copyOf(result);
    }

    public static Set<String> listApplicableRoles(SemiTransactionalHiveMetastore metastore, PrestoPrincipal principal)
    {
        return listApplicableRoles(principal, metastore::listRoleGrants)
                .stream()
                .map(RoleGrant::getRoleName)
                .collect(toSet());
    }

    public static Set<HivePrivilegeInfo> listEnabledTablePrivileges(SemiTransactionalHiveMetastore metastore, String databaseName, String tableName, ConnectorIdentity identity)
    {
        ImmutableSet.Builder<PrestoPrincipal> principals = ImmutableSet.builder();
        PrestoPrincipal userPrincipal = new PrestoPrincipal(USER, identity.getUser());
        principals.add(userPrincipal);
        listEnabledRoles(identity, metastore::listRoleGrants).stream().map(role -> new PrestoPrincipal(ROLE, role)).forEach(principals::add);
        return listTablePrivileges(metastore, databaseName, tableName, principals.build());
    }

    public static Set<HivePrivilegeInfo> listApplicableTablePrivileges(SemiTransactionalHiveMetastore metastore, String databaseName, String tableName, String user)
    {
        ImmutableSet.Builder<PrestoPrincipal> principals = ImmutableSet.builder();
        PrestoPrincipal userPrincipal = new PrestoPrincipal(USER, user);
        principals.add(userPrincipal);
        listApplicableRoles(metastore, userPrincipal).stream().map(role -> new PrestoPrincipal(ROLE, role)).forEach(principals::add);
        return listTablePrivileges(metastore, databaseName, tableName, principals.build());
    }

    private static Set<HivePrivilegeInfo> listTablePrivileges(SemiTransactionalHiveMetastore metastore, String databaseName, String tableName, Set<PrestoPrincipal> principals)
    {
        ImmutableSet.Builder<HivePrivilegeInfo> result = ImmutableSet.builder();
        for (PrestoPrincipal current : principals) {
            result.addAll(metastore.listTablePrivileges(databaseName, tableName, current));
        }
        return result.build();
    }

    public static Set<String> listEnabledRoles(ConnectorIdentity identity, Function<PrestoPrincipal, Set<RoleGrant>> listRoleGrants)
    {
        Optional<SelectedRole> role = identity.getRole();

        if (role.isPresent() && role.get().getType() == SelectedRole.Type.NONE) {
            return ImmutableSet.of("public");
        }

        PrestoPrincipal principal;
        if (!role.isPresent() || role.get().getType() == SelectedRole.Type.ALL) {
            principal = new PrestoPrincipal(USER, identity.getUser());
        }
        else {
            principal = new PrestoPrincipal(ROLE, identity.getRole().get().getRole().get());
        }

        Set<String> roles = listApplicableRoles(principal, listRoleGrants)
                .stream()
                .map(RoleGrant::getRoleName)
                .collect(toSet());

        // The admin role must be enabled explicitly. If it is, will be re-added below.
        roles.remove("admin");

        roles.add("public");

        if (principal.getType() == ROLE) {
            roles.add(principal.getName());
        }

        return ImmutableSet.copyOf(roles);
    }

    public static org.apache.hadoop.hive.metastore.api.Partition toMetastoreApiPartition(Partition partition)
    {
        org.apache.hadoop.hive.metastore.api.Partition result = new org.apache.hadoop.hive.metastore.api.Partition();
        result.setDbName(partition.getDatabaseName());
        result.setTableName(partition.getTableName());
        result.setValues(partition.getValues());
        result.setSd(makeStorageDescriptor(partition.getTableName(), partition.getColumns(), partition.getStorage()));
        result.setParameters(partition.getParameters());
        return result;
    }

    public static Database fromMetastoreApiDatabase(org.apache.hadoop.hive.metastore.api.Database database)
    {
        String ownerName = "PUBLIC";
        PrincipalType ownerType = ROLE;
        if (database.getOwnerName() != null) {
            ownerName = database.getOwnerName();
            ownerType = fromMetastoreApiPrincipalType(database.getOwnerType());
        }

        Map<String, String> parameters = database.getParameters();
        if (parameters == null) {
            parameters = ImmutableMap.of();
        }

        return Database.builder()
                .setDatabaseName(database.getName())
                .setLocation(Optional.ofNullable(database.getLocationUri()))
                .setOwnerName(ownerName)
                .setOwnerType(ownerType)
                .setComment(Optional.ofNullable(database.getDescription()))
                .setParameters(parameters)
                .build();
    }

    public static Table fromMetastoreApiTable(org.apache.hadoop.hive.metastore.api.Table table)
    {
        StorageDescriptor storageDescriptor = table.getSd();
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(table.getDbName())
                .setTableName(table.getTableName())
                .setOwner(nullToEmpty(table.getOwner()))
                .setTableType(table.getTableType())
                .setDataColumns(storageDescriptor.getCols().stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toList()))
                .setPartitionColumns(table.getPartitionKeys().stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toList()))
                .setParameters(table.getParameters() == null ? ImmutableMap.of() : table.getParameters())
                .setViewOriginalText(Optional.ofNullable(emptyToNull(table.getViewOriginalText())))
                .setViewExpandedText(Optional.ofNullable(emptyToNull(table.getViewExpandedText())));

        fromMetastoreApiStorageDescriptor(storageDescriptor, tableBuilder.getStorageBuilder(), table.getTableName());

        return tableBuilder.build();
    }

    public static Partition fromMetastoreApiPartition(org.apache.hadoop.hive.metastore.api.Partition partition)
    {
        StorageDescriptor storageDescriptor = partition.getSd();
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Partition does not contain a storage descriptor: " + partition);
        }

        Partition.Builder partitionBuilder = Partition.builder()
                .setDatabaseName(partition.getDbName())
                .setTableName(partition.getTableName())
                .setValues(partition.getValues())
                .setColumns(storageDescriptor.getCols().stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toList()))
                .setParameters(partition.getParameters());

        fromMetastoreApiStorageDescriptor(storageDescriptor, partitionBuilder.getStorageBuilder(), format("%s.%s", partition.getTableName(), partition.getValues()));

        return partitionBuilder.build();
    }

    public static HiveColumnStatistics fromMetastoreApiColumnStatistics(ColumnStatisticsObj columnStatistics)
    {
        if (columnStatistics.getStatsData().isSetLongStats()) {
            LongColumnStatsData longStatsData = columnStatistics.getStatsData().getLongStats();
            OptionalLong nullsCount = longStatsData.isSetNumNulls() ? OptionalLong.of(longStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = longStatsData.isSetNumDVs() ? OptionalLong.of(longStatsData.getNumDVs()) : OptionalLong.empty();
            return new HiveColumnStatistics(
                    longStatsData.isSetLowValue() ? Optional.of(longStatsData.getLowValue()) : Optional.empty(),
                    longStatsData.isSetHighValue() ? Optional.of(longStatsData.getHighValue()) : Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    nullsCount,
                    fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount));
        }
        else if (columnStatistics.getStatsData().isSetDoubleStats()) {
            DoubleColumnStatsData doubleStatsData = columnStatistics.getStatsData().getDoubleStats();
            OptionalLong nullsCount = doubleStatsData.isSetNumNulls() ? OptionalLong.of(doubleStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = doubleStatsData.isSetNumDVs() ? OptionalLong.of(doubleStatsData.getNumDVs()) : OptionalLong.empty();
            return new HiveColumnStatistics(
                    doubleStatsData.isSetLowValue() ? Optional.of(doubleStatsData.getLowValue()) : Optional.empty(),
                    doubleStatsData.isSetHighValue() ? Optional.of(doubleStatsData.getHighValue()) : Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    nullsCount,
                    fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount));
        }
        else if (columnStatistics.getStatsData().isSetDecimalStats()) {
            DecimalColumnStatsData decimalStatsData = columnStatistics.getStatsData().getDecimalStats();
            OptionalLong nullsCount = decimalStatsData.isSetNumNulls() ? OptionalLong.of(decimalStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = decimalStatsData.isSetNumDVs() ? OptionalLong.of(decimalStatsData.getNumDVs()) : OptionalLong.empty();
            return new HiveColumnStatistics(
                    decimalStatsData.isSetLowValue() ? fromMetastoreDecimal(decimalStatsData.getLowValue()) : Optional.empty(),
                    decimalStatsData.isSetHighValue() ? fromMetastoreDecimal(decimalStatsData.getHighValue()) : Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    nullsCount,
                    fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount));
        }
        else if (columnStatistics.getStatsData().isSetBooleanStats()) {
            BooleanColumnStatsData booleanStatsData = columnStatistics.getStatsData().getBooleanStats();
            return new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    booleanStatsData.isSetNumTrues() ? OptionalLong.of(booleanStatsData.getNumTrues()) : OptionalLong.empty(),
                    booleanStatsData.isSetNumFalses() ? OptionalLong.of(booleanStatsData.getNumFalses()) : OptionalLong.empty(),
                    booleanStatsData.isSetNumNulls() ? OptionalLong.of(booleanStatsData.getNumNulls()) : OptionalLong.empty(),
                    OptionalLong.empty());
        }
        else if (columnStatistics.getStatsData().isSetDateStats()) {
            DateColumnStatsData dateStatsData = columnStatistics.getStatsData().getDateStats();
            OptionalLong nullsCount = dateStatsData.isSetNumNulls() ? OptionalLong.of(dateStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = dateStatsData.isSetNumDVs() ? OptionalLong.of(dateStatsData.getNumDVs()) : OptionalLong.empty();
            return new HiveColumnStatistics(
                    dateStatsData.isSetLowValue() ? fromMetastoreDate(dateStatsData.getLowValue()) : Optional.empty(),
                    dateStatsData.isSetHighValue() ? fromMetastoreDate(dateStatsData.getHighValue()) : Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    nullsCount,
                    fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount));
        }
        else if (columnStatistics.getStatsData().isSetStringStats()) {
            StringColumnStatsData stringStatsData = columnStatistics.getStatsData().getStringStats();
            OptionalLong nullsCount = stringStatsData.isSetNumNulls() ? OptionalLong.of(stringStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = stringStatsData.isSetNumDVs() ? OptionalLong.of(stringStatsData.getNumDVs()) : OptionalLong.empty();
            return new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    stringStatsData.isSetMaxColLen() ? OptionalLong.of(stringStatsData.getMaxColLen()) : OptionalLong.empty(),
                    stringStatsData.isSetAvgColLen() ? OptionalDouble.of(stringStatsData.getAvgColLen()) : OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    nullsCount,
                    fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount));
        }
        else if (columnStatistics.getStatsData().isSetBinaryStats()) {
            BinaryColumnStatsData binaryStatsData = columnStatistics.getStatsData().getBinaryStats();
            return new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    binaryStatsData.isSetMaxColLen() ? OptionalLong.of(binaryStatsData.getMaxColLen()) : OptionalLong.empty(),
                    binaryStatsData.isSetAvgColLen() ? OptionalDouble.of(binaryStatsData.getAvgColLen()) : OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    binaryStatsData.isSetNumNulls() ? OptionalLong.of(binaryStatsData.getNumNulls()) : OptionalLong.empty(),
                    OptionalLong.empty());
        }
        else {
            throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + columnStatistics);
        }
    }

    public static Optional<LocalDate> fromMetastoreDate(Date date)
    {
        if (date == null) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.ofEpochDay(date.getDaysSinceEpoch()));
    }

    public static Optional<BigDecimal> fromMetastoreDecimal(@Nullable Decimal decimal)
    {
        if (decimal == null) {
            return Optional.empty();
        }
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    /**
     * Hive calculates NDV considering null as a distinct value
     */
    private static OptionalLong fromMetastoreDistinctValuesCount(OptionalLong distinctValuesCount, OptionalLong nullsCount)
    {
        if (distinctValuesCount.isPresent() && nullsCount.isPresent() && distinctValuesCount.getAsLong() > 0 && nullsCount.getAsLong() > 0) {
            return OptionalLong.of(distinctValuesCount.getAsLong() - 1);
        }
        return distinctValuesCount;
    }

    public static Set<RoleGrant> fromRolePrincipalGrants(Collection<RolePrincipalGrant> grants)
    {
        return ImmutableSet.copyOf(grants.stream().map(ThriftMetastoreUtil::fromRolePrincipalGrant).collect(toList()));
    }

    public static RoleGrant fromRolePrincipalGrant(RolePrincipalGrant grant)
    {
        return new RoleGrant(
                new PrestoPrincipal(fromMetastoreApiPrincipalType(grant.getPrincipalType()), grant.getPrincipalName()),
                grant.getRoleName(),
                grant.isGrantOption());
    }

    public static org.apache.hadoop.hive.metastore.api.PrincipalType fromPrestoPrincipalType(PrincipalType principalType)
    {
        switch (principalType) {
            case USER:
                return org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
            case ROLE:
                return org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
            default:
                throw new IllegalArgumentException("Unsupported principal type: " + principalType);
        }
    }

    public static PrincipalType fromMetastoreApiPrincipalType(org.apache.hadoop.hive.metastore.api.PrincipalType principalType)
    {
        switch (principalType) {
            case USER:
                return USER;
            case ROLE:
                return ROLE;
            default:
                throw new IllegalArgumentException("Unsupported principal type: " + principalType);
        }
    }

    public static FieldSchema toMetastoreApiFieldSchema(Column column)
    {
        return new FieldSchema(column.getName(), column.getType().getHiveTypeName().toString(), column.getComment().orElse(null));
    }

    public static Column fromMetastoreApiFieldSchema(FieldSchema fieldSchema)
    {
        return new Column(fieldSchema.getName(), HiveType.valueOf(fieldSchema.getType()), Optional.ofNullable(emptyToNull(fieldSchema.getComment())));
    }

    public static void fromMetastoreApiStorageDescriptor(StorageDescriptor storageDescriptor, Storage.Builder builder, String tablePartitionName)
    {
        SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table storage descriptor is missing SerDe info");
        }

        builder.setStorageFormat(StorageFormat.createNullable(serdeInfo.getSerializationLib(), storageDescriptor.getInputFormat(), storageDescriptor.getOutputFormat()))
                .setLocation(nullToEmpty(storageDescriptor.getLocation()))
                .setBucketProperty(HiveBucketProperty.fromStorageDescriptor(storageDescriptor, tablePartitionName))
                .setSkewed(storageDescriptor.isSetSkewedInfo() && storageDescriptor.getSkewedInfo().isSetSkewedColNames() && !storageDescriptor.getSkewedInfo().getSkewedColNames().isEmpty())
                .setSerdeParameters(serdeInfo.getParameters() == null ? ImmutableMap.of() : serdeInfo.getParameters());
    }

    private static StorageDescriptor makeStorageDescriptor(String tableName, List<Column> columns, Storage storage)
    {
        if (storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to skewed table/partition is not supported");
        }
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(tableName);
        serdeInfo.setSerializationLib(storage.getStorageFormat().getSerDeNullable());
        serdeInfo.setParameters(storage.getSerdeParameters());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(emptyToNull(storage.getLocation()));
        sd.setCols(columns.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiFieldSchema)
                .collect(toList()));
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(storage.getStorageFormat().getInputFormatNullable());
        sd.setOutputFormat(storage.getStorageFormat().getOutputFormatNullable());
        sd.setParameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.setNumBuckets(bucketProperty.get().getBucketCount());
            sd.setBucketCols(bucketProperty.get().getBucketedBy());
            if (!bucketProperty.get().getSortedBy().isEmpty()) {
                sd.setSortCols(bucketProperty.get().getSortedBy().stream()
                        .map(column -> new Order(column.getColumnName(), column.getOrder().getHiveOrder()))
                        .collect(toList()));
            }
        }

        return sd;
    }

    public static Set<HivePrivilegeInfo> parsePrivilege(PrivilegeGrantInfo userGrant)
    {
        boolean withGrantOption = userGrant.isGrantOption();
        String name = userGrant.getPrivilege().toUpperCase(ENGLISH);
        switch (name) {
            case "ALL":
                return Arrays.stream(HivePrivilegeInfo.HivePrivilege.values())
                        .map(hivePrivilege -> new HivePrivilegeInfo(hivePrivilege, withGrantOption))
                        .collect(toImmutableSet());
            case "SELECT":
                return ImmutableSet.of(new HivePrivilegeInfo(SELECT, withGrantOption));
            case "INSERT":
                return ImmutableSet.of(new HivePrivilegeInfo(INSERT, withGrantOption));
            case "UPDATE":
                return ImmutableSet.of(new HivePrivilegeInfo(UPDATE, withGrantOption));
            case "DELETE":
                return ImmutableSet.of(new HivePrivilegeInfo(DELETE, withGrantOption));
            case "OWNERSHIP":
                return ImmutableSet.of(new HivePrivilegeInfo(OWNERSHIP, withGrantOption));
            default:
                throw new IllegalArgumentException("Unsupported privilege name: " + name);
        }
    }
}
