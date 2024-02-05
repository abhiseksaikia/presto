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
package com.facebook.presto.operator;

import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.presto.common.Page;
import com.facebook.presto.execution.PageDataFetcher;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPageBackupClient
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;

    private static final PagesSerde PAGES_SERDE = testingPagesSerde();

    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (pageBufferClientCallbackExecutor != null) {
            pageBufferClientCallbackExecutor.shutdownNow();
            pageBufferClientCallbackExecutor = null;
        }
    }

    @Test
    public void testHappyPath()
    {
        Page expectedPage = new Page(100);

        DataSize expectedMaxSize = new DataSize(11, DataSize.Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(expectedMaxSize);
        URI location = URI.create("http://localhost:8080");
        PageDataFetcher client = new PageDataFetcher(new TestingHttpClient(processor, scheduler),
                location, 0);

        // fetch a page and verify
        processor.addPage(location, expectedPage);

        Iterator<List<SerializedPage>> pageIterator = client.getPages();
        if (pageIterator.hasNext()) {
            List<Page> pages = pageIterator.next().stream().map(PAGES_SERDE::deserialize)
                    .collect(toImmutableList());
            assertEquals(pages.size(), 1);
            assertPageEquals(pages.get(0), expectedPage);
        }
        // since end of buffer is not reached, hasnext should return false
        assertTrue(pageIterator.hasNext());
        assertNotNull(pageIterator.next());
        assertTrue(pageIterator.next().isEmpty());
        //add two pages
        processor.addPage(location, expectedPage);
        processor.addPage(location, expectedPage);
        List<Page> allPages = new ArrayList<>();
        for (int pageCount = 0; pageCount < 2; pageCount++) {
            List<Page> pages = pageIterator.next().stream().map(PAGES_SERDE::deserialize)
                    .collect(toImmutableList());
            allPages.addAll(pages);
        }
        assertEquals(allPages.size(), 2);
        assertTrue(pageIterator.hasNext());
        assertNotNull(pageIterator.next());
        assertTrue(pageIterator.next().isEmpty());
        //complete the buffer
        processor.setComplete(location);
        if (pageIterator.hasNext()) {
            List<SerializedPage> nextPage = pageIterator.next();
            assertNotNull(nextPage);
            assertTrue(nextPage.isEmpty());
        }
        assertFalse(pageIterator.hasNext());
    }

    private static void assertPageEquals(Page expectedPage, Page actualPage)
    {
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
    }
}
