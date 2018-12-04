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
package com.facebook.presto.dispatcher;

import com.facebook.presto.Session;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorFactory;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.StatementUtils.isTransactionControlStatement;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Objects.requireNonNull;

public class LocalDispatchQueryFactory
        implements DispatchQueryFactory
{
    private final QueryManager queryManager;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;

    private final CoordinatorLocation coordinatorLocation;
    private final ClusterSizeMonitor clusterSizeMonitor;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;
    private final WarningCollectorFactory warningCollectorFactory;
    private final ListeningExecutorService executorService;

    @Inject
    public LocalDispatchQueryFactory(
            QueryManager queryManager,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories,
            InternalNodeManager internalNodeManager,
            WarningCollectorFactory warningCollectorFactory,
            ClusterSizeMonitor clusterSizeMonitor,
            @ForQueryExecution ExecutorService executorService)

    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.executionFactories = requireNonNull(executionFactories, "executionFactories is null");
        this.warningCollectorFactory = requireNonNull(warningCollectorFactory, "warningCollectorFactory is null");

        Node currentNode = requireNonNull(internalNodeManager, "internalNodeManager is null").getCurrentNode();
        this.coordinatorLocation = new CoordinatorLocation(Optional.of(currentNode.getHttpUri()), Optional.of(currentNode.getHttpUri()));
        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");

        this.executorService = listeningDecorator(requireNonNull(executorService, "executorService is null"));
    }

    @Override
    public DispatchQuery createDispatchQuery(
            Session session,
            String query,
            PreparedQuery preparedQuery,
            String slug,
            ResourceGroupId resourceGroup,
            Executor queryExecutor)
    {
        WarningCollector warningCollector = warningCollectorFactory.create();
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                query,
                session,
                locationFactory.createQueryLocation(session.getQueryId()),
                resourceGroup,
                isTransactionControlStatement(preparedQuery.getStatement()),
                transactionManager,
                accessControl,
                executorService,
                metadata,
                warningCollector);

        queryMonitor.queryCreatedEvent(stateMachine.getBasicQueryInfo(Optional.empty()));

        ListenableFuture<QueryExecution> queryExecutionFuture = executorService.submit(() -> {
            QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(preparedQuery.getStatement().getClass());
            if (queryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + preparedQuery.getStatement().getClass().getSimpleName());
            }

            return queryExecutionFactory.createQueryExecution(preparedQuery, stateMachine, slug, warningCollector);
        });

        return new LocalDispatchQuery(
                stateMachine,
                queryExecutionFuture,
                coordinatorLocation,
                clusterSizeMonitor,
                queryExecution -> executorService.submit(() -> queryManager.createQuery(queryExecution)));
    }
}
