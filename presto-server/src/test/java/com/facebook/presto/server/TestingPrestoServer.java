package com.facebook.presto.server;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.guice.TestingJmxModule;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchModule;
import com.facebook.presto.util.InMemoryTpchBlocksProvider;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.net.HostAndPort;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceSelectorManager;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.testing.FileUtils;
import io.airlift.tracetoken.TraceTokenModule;
import org.weakref.jmx.guice.MBeanModule;

import java.io.Closeable;
import java.io.File;
import java.net.URI;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertTrue;

public class TestingPrestoServer
        implements Closeable
{
    private final File baseDataDir;
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;
    private final Metadata metadata;
    private final NodeManager nodeManager;
    private final ServiceSelectorManager serviceSelectorManager;

    public TestingPrestoServer()
            throws Exception
    {
        this(ImmutableMap.<String, String>of(), null, null);
    }

    public TestingPrestoServer(Map<String, String> properties, String environment, URI discoveryUri)
            throws Exception
    {
        baseDataDir = Files.createTempDir();

        ImmutableMap.Builder<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .putAll(properties)
                .put("storage-manager.data-directory", baseDataDir.getPath())
                .put("presto-metastore.db.type", "h2")
                .put("presto-metastore.db.filename", new File(baseDataDir, "db/MetaStore").getPath())
                .put("presto.version", "testversion");

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule(Optional.fromNullable(environment)))
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule())
                .add(new MBeanModule())
                .add(new TestingJmxModule())
                .add(new InMemoryEventModule())
                .add(new TraceTokenModule())
                .add(new FailureDetectorModule())
                .add(new ServerMainModule())
                .add(new TpchModule())
                .add(new InMemoryTpchModule());

        if (discoveryUri != null) {
            checkNotNull(environment, "environment required when discoveryUri is present");
            serverProperties.put("discovery.uri", discoveryUri.toString());
            modules.add(new DiscoveryModule());
        }
        else {
            modules.add(new TestingDiscoveryModule());
        }

        Bootstrap app = new Bootstrap(modules.build());

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties.build())
                .initialize();

        injector.getInstance(Announcer.class).start();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        ConnectorManager connectorManager = injector.getInstance(ConnectorManager.class);
        connectorManager.createConnection("default", "native", ImmutableMap.<String, String>of());
        connectorManager.createConnection("tpch", "tpch", ImmutableMap.<String, String>of());

        server = injector.getInstance(TestingHttpServer.class);
        metadata = injector.getInstance(Metadata.class);
        nodeManager = injector.getInstance(NodeManager.class);
        serviceSelectorManager = injector.getInstance(ServiceSelectorManager.class);

        refreshServiceSelectors();
    }

    @Override
    public void close()
    {
        try {
            if (lifeCycleManager != null) {
                lifeCycleManager.stop();
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        finally {
            FileUtils.deleteRecursively(baseDataDir);
        }
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    public URI resolve(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromParts(getBaseUrl().getHost(), getBaseUrl().getPort());
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public final void refreshServiceSelectors()
    {
        serviceSelectorManager.forceRefresh();
        nodeManager.refreshNodes(true);
        assertTrue(nodeManager.getCurrentNode().isPresent(), "Current node is not in active set");
    }

    private static class InMemoryTpchModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(TpchBlocksProvider.class).to(InMemoryTpchBlocksProvider.class).in(Scopes.SINGLETON);
        }
    }
}
