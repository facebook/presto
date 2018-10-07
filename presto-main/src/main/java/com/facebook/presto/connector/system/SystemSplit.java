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
package com.facebook.presto.connector.system;

import com.facebook.presto.connector.CatalogName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SystemSplit
        implements ConnectorSplit
{
    private final CatalogName catalogName;
    private final SystemTableHandle tableHandle;
    private final List<HostAddress> addresses;
    private final TupleDomain<ColumnHandle> constraint;

    public SystemSplit(CatalogName catalogName, SystemTableHandle tableHandle, HostAddress address, TupleDomain<ColumnHandle> constraint)
    {
        this(catalogName, tableHandle, ImmutableList.of(requireNonNull(address, "address is null")), constraint);
    }

    @JsonCreator
    public SystemSplit(
            @JsonProperty("connectorId") CatalogName catalogName,
            @JsonProperty("tableHandle") SystemTableHandle tableHandle,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.catalogName = requireNonNull(catalogName, "connectorId is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");

        requireNonNull(addresses, "hosts is null");
        checkArgument(!addresses.isEmpty(), "hosts is empty");
        this.addresses = ImmutableList.copyOf(addresses);
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public SystemTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", catalogName)
                .add("tableHandle", tableHandle)
                .add("addresses", addresses)
                .toString();
    }
}
