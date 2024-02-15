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
package com.facebook.presto.execution;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.operator.HttpRpcShuffleClient;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.operator.RpcShuffleClient;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.spi.HostAddress.fromUri;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.SERIALIZED_PAGE_CHECKSUM_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.isChecksumValid;
import static com.facebook.presto.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static com.facebook.presto.util.Failures.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class PageDataFetcher
{
    private static final Logger log = Logger.get(SqlTaskManager.class);
    //FIXME Do we need dynamic size here?
    public static final DataSize PAGE_FETCHER_PAGE_SIZE = new DataSize(1, DataSize.Unit.KILOBYTE);

    private final RpcShuffleClient rpcShuffleClient;
    private final long startingSeqId;
    private final URI location;
    private final Ticker ticker;
    private long lastRequestStart;
    private long veryFirstRequestStart;
    private long bytesRead;

    public PageDataFetcher(HttpClient httpClient, URI location, long startingSeqId)
    {
        this.location = location;
        this.rpcShuffleClient = new HttpRpcShuffleClient(httpClient, location);
        this.startingSeqId = startingSeqId;
        this.ticker = Ticker.systemTicker();
    }

    public synchronized void startRequest()
    {
        lastRequestStart = ticker.read();
    }

    public synchronized void success()
    {
        lastRequestStart = 0;
    }

    public PageIterator getPages()
    {
        return new PageIterator(startingSeqId);
    }

    public void close()
    {
        log.info("Going to abort result at %s, bytes read=%s, timeSinceLastRequest=%s, timeSinceVeryFirstRequest=%s", location, bytesRead, timeSinceLastRequest(), timeSinceVeryFirstRequest());
        rpcShuffleClient.abortResults(true);
        log.info("abort result successful at %s", location);
        //FIXME mark finish as true for the iterator
    }

    private class PageIterator
            implements Iterator<List<SerializedPage>>
    {
        private List<SerializedPage> nextPages;
        private boolean finished;
        private long nextToken;
        private long startingSeqId;
        private String taskInstanceId;

        public PageIterator(long startingSeqId)
        {
            this.startingSeqId = startingSeqId;
            veryFirstRequestStart = ticker.read();
            fetchNextPageBatch();
        }

        @Override
        public boolean hasNext()
        {
            return !finished;
        }

        @Override
        public List<SerializedPage> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException("No more pages.");
            }
            List<SerializedPage> currentPageBatch = nextPages;
            fetchNextPageBatch(); // Prepare the next batch of pages for the next call to next()
            return currentPageBatch;
        }

        private void fetchNextPageBatch()
                throws RuntimeException
        {
            if (finished) {
                nextPages = null;
                return;
            }
            try {
                startRequest();
                ListenableFuture<PageBufferClient.PagesResponse> resultData = rpcShuffleClient.getResults(startingSeqId, PAGE_FETCHER_PAGE_SIZE, true);
                checkArgument(resultData != null, "Failed to get results from RPC shuffle client");
                PageBufferClient.PagesResponse result = resultData.get();
                if (taskInstanceId == null) {
                    taskInstanceId = result.getTaskInstanceId();
                }
                if (result.isClientComplete()) {
                    finished = true;
                    nextPages = null;
                }
                if (!isNullOrEmpty(taskInstanceId) && !result.getTaskInstanceId().equals(taskInstanceId)) {
                    // TODO: update error message
                    throw new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, fromUri(location)));
                }
                if (result.getToken() == startingSeqId) {
                    nextPages = result.getPages();
                    nextToken = result.getNextToken();
                    if (nextPages.size() > 0) {
                        // Acknowledge the receipt of the pages after reading one batch
                        rpcShuffleClient.acknowledgeResultsAsync(result.getToken(), true);
                    }
                    // Update the starting sequence ID for the next batch
                    startingSeqId = nextToken;
                }
                else {
                    nextPages = ImmutableList.of();
                }

                for (SerializedPage page : nextPages) {
                    if (!isChecksumValid(page)) {
                        throw new PrestoException(SERIALIZED_PAGE_CHECKSUM_ERROR, format("Received corrupted serialized page from host %s", HostAddress.fromUri(location)));
                    }
                    bytesRead += page.getSizeInBytes();
                }
                success();
            }
            catch (InterruptedException e) {
                log.error(e, "InterruptedException: Failed to get page resultData from %s , seq id =%s", location, startingSeqId);
                throw new RuntimeException("InterruptedException: Failed to get page resultData", e);
            }
            catch (ExecutionException e) {
                log.error(e, "InterruptedException: Failed to get page resultData from %s , seq id =%s", location, startingSeqId);
                throw new RuntimeException("ExecutionException:Failed to get page resultData", e);
            }
            catch (Exception ex) {
                log.error(ex, "Failed to get page resultData from %s , seq id =%s, bytes read=%s, timeSinceLastRequest=%s, timeSinceVeryFirstRequest=%s", location, startingSeqId, bytesRead, timeSinceLastRequest(), timeSinceVeryFirstRequest());
                throw new RuntimeException("Failed to get page resultData", ex);
            }
        }
    }

    public long getBytesRead()
    {
        return bytesRead;
    }

    public Duration timeSinceLastRequest()
    {
        return new Duration(ticker.read() - lastRequestStart, NANOSECONDS).convertTo(MILLISECONDS);
    }

    public Duration timeSinceVeryFirstRequest()
    {
        return new Duration(ticker.read() - veryFirstRequestStart, NANOSECONDS).convertTo(MILLISECONDS);
    }
}
