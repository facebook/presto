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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.sql.analyzer.MaterializedViewCandidateExtractor;
import com.facebook.presto.sql.analyzer.MaterializedViewQueryOptimizer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MaterializedViewOptimizationRewriterUtils
{
    private MaterializedViewOptimizationRewriterUtils() {}

    public static Optional<Query> optimizeQueryUsingMaterializedView(
            Metadata metadata,
            Session session,
            SqlParser sqlParser,
            Query node)
    {
        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = getBaseToMaterializedViews();
        MaterializedViewCandidateExtractor materializedViewCandidateExtractor = new MaterializedViewCandidateExtractor(session, baseTableToMaterializedViews);
        materializedViewCandidateExtractor.process(node);
        Set<QualifiedObjectName> materializedViewCandidates = materializedViewCandidateExtractor.getMaterializedViewCandidates();
        if (materializedViewCandidates.isEmpty()) {
            return Optional.empty();
        }
        // Temporarily it returns the first candidate
        QualifiedObjectName materializedViewCandidate = materializedViewCandidates.iterator().next();
        Query optimizedQuery = getQueryWithMaterializedViewOptimization(metadata, session, sqlParser, node, materializedViewCandidate);
        return Optional.of(optimizedQuery);
    }

    private static Query getQueryWithMaterializedViewOptimization(
            Metadata metadata,
            Session session,
            SqlParser sqlParser,
            Query statement,
            QualifiedObjectName materializedViewQualifiedObjectName)
    {
        ConnectorMaterializedViewDefinition materializedView = metadata.getMaterializedView(session, materializedViewQualifiedObjectName).get();
        Table materializedViewTable = new Table(QualifiedName.of(materializedView.getTable()));

        Query materializedViewDefinition = (Query) sqlParser.createStatement(materializedView.getOriginalSql());
        Query rewriteBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer(metadata, session, sqlParser, new RowExpressionDomainTranslator(metadata), materializedViewTable, materializedViewDefinition).rewrite(statement);
        return rewriteBaseToViewQuery;
    }

    // TODO: The mapping should be fetched from metastore
    private static Map<QualifiedObjectName, List<QualifiedObjectName>> getBaseToMaterializedViews()
    {
        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = new HashMap<>();
        baseTableToMaterializedViews.put(QualifiedObjectName.valueOf("hive.tpch.lineitem_partitioned_derived_fields"), ImmutableList.of(QualifiedObjectName.valueOf("hive.tpch.lineitem_partitioned_view_derived_fields")));
        return baseTableToMaterializedViews;
    }
}
