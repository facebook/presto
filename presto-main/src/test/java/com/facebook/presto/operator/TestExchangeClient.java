package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.ExchangeClientStatus.uriGetter;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestExchangeClient
{
    @Test
    public void testHappyPath()
            throws Exception
    {
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(new DataSize(10, Unit.MEGABYTE));

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        ExchangeClient exchangeClient = new ExchangeClient(new DataSize(32, Unit.MEGABYTE),
                new DataSize(10, Unit.MEGABYTE), 1,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                ImmutableSet.of(location),
                true);


        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(3));
        assertNull(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)));
        assertEquals(exchangeClient.isClosed(), true);
        if (exchangeClient.getBufferedPageCount() != 0) {
            System.out.println(exchangeClient.pageBuffer);
            assertEquals(exchangeClient.getBufferedPageCount(), 0);
        }
        assertTrue(exchangeClient.getBufferBytes() == 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        assertStatus(exchangeClient.getStatus().get(0), location, "closed", 3, 2, 2, "queued");
    }

    @Test
    public void testAddLocation()
            throws Exception
    {
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(new DataSize(10, Unit.MEGABYTE));

        ExchangeClient exchangeClient = new ExchangeClient(new DataSize(32, Unit.MEGABYTE),
                new DataSize(10, Unit.MEGABYTE), 1,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                ImmutableSet.<URI>of(),
                false);

        URI location1 = URI.create("http://localhost:8081");
        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));
        processor.setComplete(location1);
        exchangeClient.addLocation(location1);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(3));

        assertNull(exchangeClient.getNextPage(new Duration(10, TimeUnit.MILLISECONDS)));
        assertEquals(exchangeClient.isClosed(), false);

        URI location2 = URI.create("http://localhost:8082");
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);
        exchangeClient.addLocation(location2);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(4));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(5));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(6));

        assertNull(exchangeClient.getNextPage(new Duration(10, TimeUnit.MILLISECONDS)));
        assertEquals(exchangeClient.isClosed(), false);

        exchangeClient.noMoreLocations();
        assertEquals(exchangeClient.isClosed(), true);

        ImmutableMap<URI,ExchangeClientStatus> statuses = uniqueIndex(exchangeClient.getStatus(), uriGetter());
        assertStatus(statuses.get(location1), location1, "closed", 3, 2, 2, "queued");
        assertStatus(statuses.get(location2), location2, "closed", 3, 2, 2, "queued");
    }

    @Test
    public void testBufferLimit()
            throws Exception
    {
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(new DataSize(1, Unit.BYTE));

        URI location = URI.create("http://localhost:8080");

        ExchangeClient exchangeClient = new ExchangeClient(new DataSize(1, Unit.BYTE),
                new DataSize(1, Unit.BYTE),
                1,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                ImmutableSet.of(location),
                true);
        assertEquals(exchangeClient.isClosed(), false);

        // add a pages
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        // start fetching pages
        exchangeClient.scheduleRequestIfNecessary();
        // wait for a page to be fetched
        do {
            // there is no thread coordination here, so sleep is the best we can do
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } while (exchangeClient.getBufferedPageCount() == 0);

        // client should have sent a single request for a single page
        assertEquals(exchangeClient.getBufferedPageCount(), 1);
        assertTrue(exchangeClient.getBufferBytes() > 0);
        assertStatus(exchangeClient.getStatus().get(0), location, "queued", 1, 1, 1, "queued");

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.getNextPage(new Duration(0, TimeUnit.SECONDS)), createPage(1));
        do {
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } while (exchangeClient.getBufferedPageCount() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().get(0), location, "queued", 2, 2, 2, "queued");
        assertEquals(exchangeClient.getBufferedPageCount(), 1);
        assertTrue(exchangeClient.getBufferBytes() > 0);

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.getNextPage(new Duration(0, TimeUnit.SECONDS)), createPage(2));
        do {
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } while (exchangeClient.getBufferedPageCount() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().get(0), location, "queued", 3, 3, 3, "queued");
        assertEquals(exchangeClient.getBufferedPageCount(), 1);
        assertTrue(exchangeClient.getBufferBytes() > 0);

        // remove last page
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(3));

        //  wait for client to decide there are no more pages
        assertNull(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)));
        assertEquals(exchangeClient.getBufferedPageCount(), 0);
        assertTrue(exchangeClient.getBufferBytes() == 0);
        assertEquals(exchangeClient.isClosed(), true);
        assertStatus(exchangeClient.getStatus().get(0), location, "closed", 3, 4, 4, "queued");
    }

    @Test
    public void testClose()
            throws Exception
    {
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(new DataSize(1, Unit.BYTE));

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));

        ExchangeClient exchangeClient = new ExchangeClient(new DataSize(1, Unit.BYTE),
                new DataSize(1, Unit.BYTE), 1,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                ImmutableSet.of(location),
                true);

        // fetch a page
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.HOURS)), createPage(1));

        // close client while pages are still available
        exchangeClient.close();
        assertEquals(exchangeClient.isClosed(), true);
        assertNull(exchangeClient.getNextPage(new Duration(0, TimeUnit.SECONDS)));
        assertEquals(exchangeClient.getBufferedPageCount(), 0);
        assertEquals(exchangeClient.getBufferBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        ExchangeClientStatus clientStatus = exchangeClient.getStatus().get(0);
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), "closed", "status");
        assertEquals(clientStatus.getPagesReceived(), 2, "pagesReceived");
        assertEquals(clientStatus.getRequestsScheduled(), 2, "requestsScheduled");
        assertEquals(clientStatus.getRequestsCompleted(), 2, "requestsCompleted");
        assertEquals(clientStatus.getHttpRequestState(), "queued", "httpRequestState");
    }

    private Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }

    private void assertPageEquals(Page actualPage, Page expectedPage)
    {
        assertNotNull(actualPage);
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
    }

    private void assertStatus(ExchangeClientStatus clientStatus,
            URI location,
            String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            String httpRequestState)
    {
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), status, "status");
        assertEquals(clientStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(clientStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(clientStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(clientStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }
}
