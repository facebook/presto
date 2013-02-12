package com.facebook.presto.hive;

import com.facebook.prism.namespaceservice.PrismClient;
import com.facebook.swift.service.ThriftClient;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

@ThreadSafe
public class PrismClientProvider
{
    private final ThriftClient<PrismClient> thriftClient;
    private final SmcLookup smcLookup;
    private final String prismSmcTier;

    @Inject
    public PrismClientProvider(
            ThriftClient<PrismClient> thriftClient,
            SmcLookup smcLookup,
            PrismHiveConfig config)
    {
        this.thriftClient = checkNotNull(thriftClient, "thriftClient is null");
        this.smcLookup = checkNotNull(smcLookup, "smcLookup is null");
        prismSmcTier = checkNotNull(config, "config is null").getPrismSmcTier();
    }

    public PrismClient get()
    {
        List<HostAndPort> services = smcLookup.getServices(prismSmcTier);
        if (services.isEmpty()) {
            throw new RuntimeException(format("No prism servers available for tier '%s'", prismSmcTier));
        }

        TTransportException lastException = null;
        for (HostAndPort service : shuffle(services)) {
            try {
                return thriftClient.open(service);
            }
            catch (TTransportException e) {
                lastException = e;
            }
        }
        throw new RuntimeException("Unable to connect to any prism servers", lastException);
    }

    private static <T> List<T> shuffle(Iterable<T> iterable)
    {
        List<T> list = Lists.newArrayList(iterable);
        Collections.shuffle(list);
        return list;
    }
}
