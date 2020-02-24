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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.event.DeterminismAnalysisRun;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.getColumns;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.match;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.setupAndRun;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.teardownSafely;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_DATA_CHANGED;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_INCONSISTENT_SCHEMA;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_QUERY_FAILURE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_CATALOG;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_COLUMNS;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_LIMIT_CLAUSE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_ROW_COUNT;
import static com.facebook.presto.verifier.framework.VerifierUtil.callWithQueryStatsConsumer;
import static com.facebook.presto.verifier.framework.VerifierUtil.runWithQueryStatsConsumer;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeterminismAnalyzer
{
    private final SourceQuery sourceQuery;
    private final PrestoAction prestoAction;
    private final QueryRewriter queryRewriter;
    private final ChecksumValidator checksumValidator;
    private final TypeManager typeManager;
    private final VerificationContext verificationContext;

    private boolean runTeardown;
    private int maxAnalysisRuns;
    private Set<String> nonDeterministicCatalogs;
    private boolean handleLimitQuery;

    public DeterminismAnalyzer(
            SourceQuery sourceQuery,
            PrestoAction prestoAction,
            QueryRewriter queryRewriter,
            ChecksumValidator checksumValidator,
            TypeManager typeManager,
            VerificationContext verificationContext,
            DeterminismAnalyzerConfig config)
    {
        this.sourceQuery = requireNonNull(sourceQuery, "sourceQuery is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.verificationContext = requireNonNull(verificationContext, "verificationContext is null");

        this.runTeardown = config.isRunTeardown();
        this.maxAnalysisRuns = config.getMaxAnalysisRuns();
        this.nonDeterministicCatalogs = ImmutableSet.copyOf(config.getNonDeterministicCatalogs());
        this.handleLimitQuery = config.isHandleLimitQuery();
    }

    protected DeterminismAnalysis analyze(QueryBundle control, ChecksumResult controlChecksum)
    {
        // Handle mutable catalogs
        if (isNonDeterministicCatalogReferenced(control.getQuery())) {
            return NON_DETERMINISTIC_CATALOG;
        }

        List<Column> columns = getColumns(prestoAction, typeManager, control.getTableName());
        List<QueryBundle> queryBundles = new ArrayList<>();

        try {
            // Rerun control query
            for (int i = 0; i < maxAnalysisRuns; i++) {
                QueryBundle queryBundle = queryRewriter.rewriteQuery(sourceQuery.getControlQuery(), CONTROL);
                queryBundles.add(queryBundle);
                DeterminismAnalysisRun.Builder run = verificationContext.startDeterminismAnalysisRun().setTableName(queryBundle.getTableName().toString());

                runWithQueryStatsConsumer(() -> setupAndRun(prestoAction, queryBundle, true), stats -> run.setQueryId(stats.getQueryId()));

                Query checksumQuery = checksumValidator.generateChecksumQuery(queryBundle.getTableName(), columns);
                ChecksumResult testChecksum = getOnlyElement(callWithQueryStatsConsumer(
                        () -> DataVerificationUtil.executeChecksumQuery(prestoAction, checksumQuery),
                        stats -> run.setChecksumQueryId(stats.getQueryId())).getResults());

                DeterminismAnalysis analysis = matchResultToDeterminism(match(checksumValidator, columns, columns, controlChecksum, testChecksum));
                if (analysis != DETERMINISTIC) {
                    return analysis;
                }
            }

            // Handle limit query
            LimitQueryDeterminismAnalysis limitQueryAnalysis = new LimitQueryDeterminismAnalyzer(prestoAction, handleLimitQuery)
                    .analyze(control, controlChecksum.getRowCount(), verificationContext);

            switch (limitQueryAnalysis) {
                case NON_DETERMINISTIC:
                    return NON_DETERMINISTIC_LIMIT_CLAUSE;
                case NOT_RUN:
                case DETERMINISTIC:
                    return DETERMINISTIC;
                case FAILED_DATA_CHANGED:
                    return ANALYSIS_FAILED_DATA_CHANGED;
                default:
                    throw new IllegalArgumentException(format("Invalid limitQueryAnalysis: %s", limitQueryAnalysis));
            }
        }
        catch (QueryException qe) {
            return ANALYSIS_FAILED_QUERY_FAILURE;
        }
        catch (Throwable t) {
            return ANALYSIS_FAILED;
        }
        finally {
            if (runTeardown) {
                queryBundles.forEach(bundle -> teardownSafely(prestoAction, bundle));
            }
        }
    }

    private DeterminismAnalysis matchResultToDeterminism(MatchResult matchResult)
    {
        switch (matchResult.getMatchType()) {
            case MATCH:
                return DETERMINISTIC;
            case SCHEMA_MISMATCH:
                return ANALYSIS_FAILED_INCONSISTENT_SCHEMA;
            case ROW_COUNT_MISMATCH:
                return NON_DETERMINISTIC_ROW_COUNT;
            case COLUMN_MISMATCH:
                return NON_DETERMINISTIC_COLUMNS;
            default:
                throw new IllegalArgumentException(format("Invalid MatchResult: %s", matchResult));
        }
    }

    @VisibleForTesting
    boolean isNonDeterministicCatalogReferenced(Statement statement)
    {
        if (nonDeterministicCatalogs.isEmpty()) {
            return false;
        }

        AtomicBoolean nonDeterministicCatalogReferenced = new AtomicBoolean();
        new NonDeterministicCatalogVisitor().process(statement, nonDeterministicCatalogReferenced);
        return nonDeterministicCatalogReferenced.get();
    }

    private class NonDeterministicCatalogVisitor
            extends AstVisitor<Void, AtomicBoolean>
    {
        protected Void visitNode(Node node, AtomicBoolean context)
        {
            node.getChildren().forEach(child -> process(child, context));
            return null;
        }

        protected Void visitTable(Table node, AtomicBoolean nonDeterministicCatalogReferenced)
        {
            if (node.getName().getParts().size() == 3 && nonDeterministicCatalogs.contains(node.getName().getParts().get(0))) {
                nonDeterministicCatalogReferenced.set(true);
            }
            return null;
        }
    }
}
