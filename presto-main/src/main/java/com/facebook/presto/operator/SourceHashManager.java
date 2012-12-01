/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.server.HttpTaskClient;
import com.facebook.presto.server.QueryDriverProvider;
import com.facebook.presto.server.QueryDriversOperator;
import com.facebook.presto.server.QueryTaskInfo;
import com.facebook.presto.split.ExchangeSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class SourceHashManager
{
    private final ExecutorService executor;
    private final ApacheHttpClient httpClient;

    @GuardedBy("this")
    private final Map<String, SourceHashProvider> hashProvidersByQueryId = new HashMap<>();
    private final JsonCodec<QueryTaskInfo> queryTaskInfoCodec;

    @Inject
    public SourceHashManager(ExecutorService executor, JsonCodec<QueryTaskInfo> queryTaskInfoCodec1)
    {
        this.executor = executor;
        httpClient = new ApacheHttpClient(new HttpClientConfig()
                .setConnectTimeout(new Duration(5, TimeUnit.MINUTES))
                .setReadTimeout(new Duration(5, TimeUnit.MINUTES)));

        queryTaskInfoCodec = queryTaskInfoCodec1;
    }

    public synchronized SourceHashProvider getSourceHashProvider(
            String queryId,
            int hashChannel,
            int expectedPositions,
            List<Split> splits,
            final List<TupleInfo> outputTupleInfos)
    {
        SourceHashProvider sourceHashProvider = hashProvidersByQueryId.get(queryId);
        if (sourceHashProvider == null) {
            QueryDriversOperator operator = new QueryDriversOperator(10,
                    Iterables.transform(splits, new Function<Split, QueryDriverProvider>()
                    {
                        @Override
                        public QueryDriverProvider apply(Split split)
                        {
                            ExchangeSplit exchangeSplit = (ExchangeSplit) split;
                            return new HttpTaskClient(
                                    exchangeSplit.getLocation().toString(), // todo add taskId to exchange split
                                    exchangeSplit.getLocation(),
                                    "out",
                                    outputTupleInfos, httpClient,
                                    executor,
                                    queryTaskInfoCodec
                            );
                        }
                    })
            );
            sourceHashProvider = new SourceHashProvider(operator, hashChannel, expectedPositions);
            hashProvidersByQueryId.put(queryId, sourceHashProvider);
        }
        return sourceHashProvider;
    }

    public synchronized void dropSourceHashProvider(String queryId)
    {
        hashProvidersByQueryId.remove(queryId);
    }
}
