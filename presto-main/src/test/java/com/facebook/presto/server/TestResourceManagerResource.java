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
package com.facebook.presto.server;

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.testing.Closeables.closeQuietly;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestResourceManagerResource
{
    private TestingPrestoServer server;
    private HttpClient client;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        client = new JettyHttpClient();
    }

    @AfterMethod
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
    }

    @Test
    public void testGetGroups()
            throws Exception
    {
        List<ResourceGroupInfo> resourceGroups = client.execute(
                prepareGet().setUri(server.resolve("/v1/resource-group")).build(),
                createJsonResponseHandler(listJsonCodec(ResourceGroupInfo.class)));

        assertTrue(resourceGroups.isEmpty());
    }
}
