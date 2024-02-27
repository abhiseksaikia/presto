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
import com.facebook.presto.operator.PageBufferClient.PagesResponse;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    public static final DataSize PAGE_FETCHER_PAGE_SIZE = new DataSize(500, DataSize.Unit.MEGABYTE);
    //Setting a high value for test to avoid any buffer size accounting issue introducing noise in test, use config and set a proper value
    public static final DataSize HTTP_MAX_CONTENT_LENGTH = new DataSize(600, DataSize.Unit.MEGABYTE);

    private final RpcShuffleClient rpcShuffleClient;
    private final long startingSeqId;
    private final URI location;
    private final Ticker ticker;
    private long lastRequestStart;
    private long veryFirstRequestStart;
    private long bytesRead;
    private Duration endToEndTime;
    private Duration lastRequestDuration;

    public PageDataFetcher(HttpClient httpClient, URI location, long startingSeqId)
    {
        this.location = location;
        this.rpcShuffleClient = new HttpRpcShuffleClient(httpClient, location);
        this.startingSeqId = startingSeqId;
        this.ticker = Ticker.systemTicker();
        this.endToEndTime = new Duration(0, TimeUnit.SECONDS);
        this.lastRequestDuration = new Duration(0, TimeUnit.SECONDS);
    }

    public synchronized void startRequest()
    {
        lastRequestStart = ticker.read();
    }

    public synchronized void success()
    {
        lastRequestDuration = new Duration(ticker.read() - lastRequestStart, NANOSECONDS).convertTo(MILLISECONDS);
    }

    public PageIterator getPages()
    {
        return new PageIterator(startingSeqId);
    }

    public void close()
    {
        log.info("Going to abort result at %s, bytes read=%s, timeSinceLastRequest=%s, timeSinceVeryFirstRequest=%s", location, bytesRead, timeSinceLastRequest(), timeSinceVeryFirstRequest());
        rpcShuffleClient.abortResults(true);
        endToEndTime = timeSinceVeryFirstRequest();
        log.info("abort result successful at %s, end to end time =%s", location, endToEndTime);
        //FIXME mark finish as true for the iterator
    }

    private class PageIterator
            implements Iterator<List<SerializedPage>>
    {
        private List<SerializedPage> nextPages;
        private boolean finished;
        private long token;
        private String taskInstanceId;

        public PageIterator(long token)
        {
            this.token = token;
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
                PagesResponse result = getPage();
                if (taskInstanceId == null) {
                    taskInstanceId = result.getTaskInstanceId();
                }
                if (result.isClientComplete()) {
                    log.info("result is completed for location %s", location);
                    finished = true;
                    nextPages = null;
                    return;
                }
                if (!isNullOrEmpty(taskInstanceId) && !result.getTaskInstanceId().equals(taskInstanceId)) {
                    // TODO: update error message
                    throw new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, fromUri(location)));
                }
                if (result.getToken() == token) {
                    nextPages = result.getPages();
                    long nextToken = result.getNextToken();
                    if (nextPages.size() > 0) {
                        log.info("ack result data for token =%s location =%s", token, location);
                        // Acknowledge the receipt of the pages after reading one batch
                        rpcShuffleClient.acknowledgeResultsAsync(result.getToken(), true);
                    }
                    else {
                        log.info("nextPages are empty for startingSeqId =%s location =%s", token, location);
                    }
                    // Update the starting sequence ID for the next batch
                    token = nextToken;
                }
                else {
                    log.info("result has different token than what is asked for, result token = %s startingSeqId =%s location =%s", result.getToken(), token, location);
                    nextPages = ImmutableList.of();
                }

                for (SerializedPage page : nextPages) {
                    if (!isChecksumValid(page)) {
                        throw new PrestoException(SERIALIZED_PAGE_CHECKSUM_ERROR, format("Received corrupted serialized page from host %s", HostAddress.fromUri(location)));
                    }
                    bytesRead += page.getRetainedSizeInBytes();
                }
                success();
            }
            catch (InterruptedException e) {
                log.error(e, "InterruptedException: Failed to get page resultData from %s , seq id =%s", location, token);
                throw new RuntimeException("InterruptedException: Failed to get page resultData", e);
            }
            catch (ExecutionException e) {
                log.error(e, "InterruptedException: Failed to get page resultData from %s , seq id =%s", location, token);
                throw new RuntimeException("ExecutionException:Failed to get page resultData", e);
            }
            catch (Exception ex) {
                log.error(ex, "Failed to get page resultData from %s , seq id =%s, bytes read=%s, timeSinceLastRequest=%s, timeSinceVeryFirstRequest=%s", location, token, bytesRead, timeSinceLastRequest(), timeSinceVeryFirstRequest());
                throw new RuntimeException("Failed to get page resultData", ex);
            }
        }

        private PagesResponse getPage()
                throws InterruptedException, ExecutionException
        {
            //FIXME use some library
            for (int retry = 1; retry <= 3; retry++) {
                try {
                    ListenableFuture<PagesResponse> resultData = rpcShuffleClient.getResults(token, PAGE_FETCHER_PAGE_SIZE, true);
                    checkArgument(resultData != null, "Failed to get results from RPC shuffle client");
                    PagesResponse result = resultData.get(20, TimeUnit.SECONDS);
                    return result;
                }
                catch (TimeoutException e) {
                    log.error(e, "Attempt % for location % ran into timeout error", retry, location);
                }
            }
            throw new RuntimeException(String.format("Timed out while trying to get result for seq = %s from %s", location, token));
        }
    }

    public long getBytesRead()
    {
        return bytesRead;
    }

    public Duration timeSinceLastRequest()
    {
        return lastRequestDuration;
    }

    public Duration timeSinceVeryFirstRequest()
    {
        return new Duration(ticker.read() - veryFirstRequestStart, NANOSECONDS).convertTo(MILLISECONDS);
    }
}
