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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.discovery.client.DiscoveryLookupClient;
import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.airlift.discovery.client.ServiceDescriptors;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.PageData;
import com.facebook.presto.execution.PageDataFetcher;
import com.facebook.presto.execution.PageKey;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.executor.QueryRecoveryDebugInfo;
import com.facebook.presto.execution.executor.QueryRecoveryState;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.operator.ForPageTransfer;
import com.facebook.presto.operator.HttpRpcShuffleClient;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.http.client.HttpStatus.familyForStatusCode;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.ServerConfig.POOL_TYPE;
import static com.facebook.presto.spi.NodePoolType.DATA;
import static com.facebook.presto.spi.NodePoolType.DEFAULT;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_CACHE_UNAVAILABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class BackupPageManager
{
    private static final Logger log = Logger.get(BackupPageManager.class);

    private final HttpClient httpClient;
    private final DiscoveryLookupClient lookupClient;
    private final LocationFactory locationFactory;
    //added for local testing
    private Optional<URL> dataNodeBaseURL = Optional.empty();
    private final JsonCodec<PageInitUploadRequest> pageInitUploadRequestJsonCodec = JsonCodec.jsonCodec(PageInitUploadRequest.class);

    //added for fault injection
    private final InternalNodeManager nodeManager;
    private final EventListenerManager eventListenerManager;
    private static final long MAX_SIZE = 24L * 1024 * 1024 * 1024; // 24 GB
    private static final long ESTIMATED_SIZE_PER_ENTRY = 150 * 1024 * 1024; // 150 MB
    private final Cache<PageKey, PageData> pageCache;
    //private final ConcurrentMap<PageKey, PageData> pageCache;
    private final Optional<ScheduledExecutorService> pageDownloadScheduler;

    @Inject
    public BackupPageManager(@ForPageTransfer HttpClient httpClient, InternalNodeManager nodeManager, LocationFactory locationFactory, DiscoveryLookupClient lookupClient, EventListenerManager eventListenerManager, ServerConfig serverConfig)
    {
        this.httpClient = httpClient;
        this.lookupClient = lookupClient;
        this.locationFactory = locationFactory;
        this.nodeManager = nodeManager;
        this.eventListenerManager = eventListenerManager;
        if (serverConfig.getPoolType() == DATA) {
            this.pageCache = CacheBuilder.newBuilder()
                    .concurrencyLevel(200)
                    .maximumSize(MAX_SIZE / ESTIMATED_SIZE_PER_ENTRY)
                    .expireAfterWrite(30, TimeUnit.MINUTES)
                    .build();
            //FIXME use rocksdb?
            //this.pageCache = new ConcurrentHashMap<>();
            pageDownloadScheduler = Optional.of(newScheduledThreadPool(100, threadsNamed("task-page-download-%s")));
        }
        else {
            //FIXME, should we use Optional
            this.pageCache = null;
            pageDownloadScheduler = Optional.empty();
        }
    }

    private ServiceDescriptor getDataNodeService()
    {
        try {
            ListenableFuture<ServiceDescriptors> services = lookupClient.getServices("presto");
            Optional<ServiceDescriptor> dataNodeServiceDescriptor = services.get().getServiceDescriptors().stream()
                    .filter(serviceDescriptor -> NodePoolType.valueOf(serviceDescriptor.getProperties().getOrDefault("pool_type", "DEFAULT")) == DATA)
                    .findFirst();
            checkArgument(dataNodeServiceDescriptor.isPresent(), "Data node not found");
            return dataNodeServiceDescriptor.get();
        }
        catch (ExecutionException e) {
            log.error(e, "Failed to discover data node");
            throw new RuntimeException("Failed to discover data node");
        }
        catch (InterruptedException e) {
            log.warn(e, "Failed to discover data node");
            throw new RuntimeException("Failed to discover data node", e);
        }
    }

    public ListenableFuture<PageBufferClient.PagesResponse> getResults(URI location, DataSize maxResponseSize)
    {
        log.info("Get result from data node %s", location);
        return httpClient.executeAsync(
                prepareGet()
                        .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                        .setUri(location).build(),
                new HttpRpcShuffleClient.PageResponseHandler(true, true));
    }

    public Optional<URI> getBackupAsyncPageTransportLocation(URI location, boolean asyncPageTransportEnabled)
    {
        // if location is preempted, should we build a redirect url here?
        if (asyncPageTransportEnabled) {
            // rewrite location for http request to get task results in async mode
            // new URL cannot replace v1/task completely, v1/task/async is only used to get task results
            String path = location.getPath().replace("v1/task", "v1/task/async");
            return Optional.of(HttpUriBuilder.uriBuilderFrom(location).replacePath(path).build());
        }
        else {
            return Optional.empty();
        }
    }

    public ListenableFuture<Response> requestPageUpload(TaskId taskId, String taskInstanceID, String bufferID, int numberOfPages, long currentSequenceID)
    {
        log.info("Client::initPageUpload task : %s , taskInstanceId:%s, bufferID:%s, page size: %s", taskId, taskInstanceID, bufferID, numberOfPages);
        //construct the space trequired to store the numberOfPages
        URI taskLocation = getTaskLocation(taskId);
        URI internalUri = nodeManager.getCurrentNode().getInternalUri();
        //FIXME URL is changed now, provide bufferLocation
        URI requestURI = uriBuilderFrom(taskLocation)
                .appendPath(taskInstanceID)
                .appendPath("buffers")
                .appendPath(bufferID)
                .appendPath(String.valueOf(currentSequenceID))
                .appendPath(String.valueOf(numberOfPages))
                .build();
        URI pageLocation = uriBuilderFrom(internalUri)
                .appendPath("/v1/task")
                .appendPath(taskId.toString())
                .appendPath("results")
                .appendPath(bufferID)
                .build();
        log.info("Client::initPageUpload requestURI : %s , pageLocation:%s", requestURI, pageLocation);

        PageInitUploadRequest initUploadRequest = new PageInitUploadRequest(pageLocation);
        byte[] taskUpdateRequestJson = pageInitUploadRequestJsonCodec.toBytes(initUploadRequest);

        Request request = setContentTypeHeaders(false, preparePost())
                .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestJson))
                .setUri(requestURI).build();
        // Define a ResponseHandler
        ResponseHandler<Response, RuntimeException> responseHandler = new ResponseHandler<Response, RuntimeException>()
        {
            @Override
            public Response handleException(Request request, Exception exception)
            {
                eventListenerManager.trackPreemptionLifeCycle(
                        taskId,
                        QueryRecoveryDebugInfo.builder()
                                .outputBufferID(bufferID)
                                .state(QueryRecoveryState.PAGE_TRANSFER_INIT_FAILED)
                                .extraInfo(ImmutableMap.of("msg", exception.getMessage()))
                                .build());
                log.error(exception, "Client::initPageUpload failed for requestURI : %s , pageLocation:%s, error = %s", requestURI, pageLocation, exception.getMessage());
                throw propagate(request, exception);
            }

            @Override
            public Response handle(Request request, Response response)
            {
                eventListenerManager.trackPreemptionLifeCycle(
                        taskId,
                        QueryRecoveryDebugInfo.builder()
                                .outputBufferID(bufferID)
                                .state(QueryRecoveryState.PAGE_TRANSFER_INIT_SUCCESS)
                                .build());
                log.info("Client::initPageUpload succeeded for requestURI : %s , pageLocation:%s", requestURI, pageLocation);
                return response;
            }
        };
        return httpClient.executeAsync(request, responseHandler);
    }

    private static NodePoolType getPoolType(ServiceDescriptor service)
    {
        if (!service.getProperties().containsKey(POOL_TYPE)) {
            return DEFAULT;
        }
        return NodePoolType.valueOf(service.getProperties().get(POOL_TYPE));
    }

    /*public void uploadPages(TaskId taskId, String taskInstanceId, String bufferID, LinkedList<SerializedPageReference> serializedPage, int pageBucketIndex, long token)
    {
        log.info("Client::uploadPages task : %s , taskInstanceId:%s, bufferID:%s, page size: %s", taskId, taskInstanceId, bufferID, serializedPage.size());
        //FIXME optimize it
        URI taskLocation = getTaskLocation(taskId);
        //{taskId}/{taskInstanceId}/buffers/{bufferId}/{token}
        URI requestURI = uriBuilderFrom(taskLocation)
                .appendPath(taskInstanceId)
                .appendPath("buffers")
                .appendPath(bufferID)
                .appendPath(String.valueOf(token))
                .build();
        PageUploadRequest pageUploadRequest = new PageUploadRequest(serialize(serializedPage), token, 0);

        HttpURLConnection connection = null;
        try {
            byte[] pageUploadJson = pageUploadRequestJsonCodec.toBytes(pageUploadRequest);

            connection = (HttpURLConnection) requestURI.toURL().openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setChunkedStreamingMode(0); // Enable chunked transfer mode

            try (OutputStream out = connection.getOutputStream()) {
                out.write(pageUploadJson);
            }

            // Check response
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed to upload pages, server responded with: " + responseCode);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Error while uploading pages: ", e);
        }
        finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }*/

    public URI getBackupBufferLocation(TaskId taskId, String bufferID)
    {
        initDataNode();
        if (dataNodeBaseURL.isPresent()) {
            try {
                HttpUriBuilder builder = uriBuilderFrom(dataNodeBaseURL.get().toURI());
                return builder.appendPath("/v1/task")
                        .appendPath(taskId.toString())
                        .appendPath("results")
                        .appendPath(bufferID)
                        .build();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException("Invalid data node url" + dataNodeBaseURL.get());
            }
        }
        Optional<InternalNode> dataNode = nodeManager.getNodes(NodeState.ACTIVE).stream().filter(node -> node.getPoolType() == NodePoolType.DATA).findFirst();
        if (dataNode.isPresent()) {
            //hack for local
            InternalNode internalNode = dataNode.get();
            URI taskLocation = this.locationFactory.createTaskLocation(internalNode, taskId);
            return uriBuilderFrom(taskLocation).appendPath(bufferID).build();
        }
        throw new RuntimeException("Data node not available to upload pages");
    }

    private void initDataNode()
    {
        if (dataNodeBaseURL.isPresent()) {
            return;
        }
        synchronized (this) {
            try {
                if (dataNodeBaseURL.isPresent()) {
                    return;
                }
                dataNodeBaseURL = Optional.of(new URL(getDataNodeService().getProperties().get("http")));
            }
            catch (MalformedURLException e) {
                log.error(e, "unable to get data node");
                throw new RuntimeException("unable to get data node", e);
            }
        }
    }

    public URI getTaskLocation(TaskId taskId)
    {
        initDataNode();
        if (dataNodeBaseURL.isPresent()) {
            try {
                HttpUriBuilder builder = uriBuilderFrom(dataNodeBaseURL.get().toURI());
                return builder.appendPath("/v1/task").appendPath(taskId.toString()).build();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException("Invalid data node url" + dataNodeBaseURL.get());
            }
        }
        Optional<InternalNode> dataNode = nodeManager.getNodes(NodeState.ACTIVE).stream().filter(node -> node.getPoolType() == NodePoolType.DATA).findFirst();
        if (dataNode.isPresent()) {
            //hack for local
            InternalNode internalNode = dataNode.get();
            URI taskLocation = this.locationFactory.createTaskLocation(internalNode, taskId);
            return taskLocation;
        }
        throw new RuntimeException("Data node not available to upload pages");
    }

    //FIXME this is purely for local debugging, make the smc based remote shutdown working for local environment and remove this
    public boolean isRemoteHostShutdown(String remoteWorker)
    {
        try {
            Optional<String> nodeType = Optional.ofNullable(System.getProperty("node_type"));
            if (!nodeType.isPresent()) {
                return false;
            }
            File shutdownFile = new File("/tmp/shutdown");
            if (!shutdownFile.exists()) {
                return false;
            }
            List<String> shutdownURIs = Files.readLines(shutdownFile, UTF_8);
            if (shutdownURIs.size() == 1) {
                String shutdownBaseURL = shutdownURIs.get(0);
                if (shutdownBaseURL.equals(remoteWorker)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public ListenableFuture<?> abortResult(TaskId taskId, String bufferID)
    {
        URI taskLocation = getTaskLocation(taskId);
        URI requestURI = uriBuilderFrom(taskLocation)
                .appendPath("results")
                .appendPath(bufferID)
                .build();
        return httpClient.executeAsync(prepareDelete().setUri(requestURI).build(), createStatusResponseHandler());
    }

    public void acknowledgeResultsAsync(TaskId taskId, String bufferID, long nextToken)
    {
        URI taskLocation = getTaskLocation(taskId);
        URI requestURI = uriBuilderFrom(taskLocation)
                .appendPath("results")
                .appendPath(bufferID)
                .appendPath(String.valueOf(nextToken))
                .build();
        httpClient.executeAsync(prepareGet().setUri(requestURI).build(), new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                log.debug(exception, "Acknowledge request failed: %s", requestURI);
                return null;
            }

            @Override
            public Void handle(Request request, Response response)
            {
                if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                    log.debug("Unexpected acknowledge response code: %s", response.getStatusCode());
                }
                return null;
            }
        });
    }

    //FIXME hack, no need to expose the client
    public HttpClient getHttpClient()
    {
        return httpClient;
    }

    // FIXME server processing, move it to a different class
    public void processUploadRequest(URI bufferLocation, TaskId taskId, String taskInstanceID, String bufferId, long token)
    {
        checkArgument(pageCache != null, "cache not available");
        // init entry into the cache
        log.info("initializeUploadPages called for task = %s , bufferId = %s, bufferLocation =%s", taskId.toString(), bufferId, bufferLocation);
        eventListenerManager.trackPreemptionLifeCycle(
                taskId,
                QueryRecoveryDebugInfo.builder()
                        .outputBufferID(bufferId)
                        .state(QueryRecoveryState.DATA_PAGE_TRANSFER_INIT_RECEIVED)
                        .build());
        pageDownloadScheduler.get().execute(() -> {
            try {
                //temporary placeholder to provide empty page to consumer before page data is fetched from worker
                pageCache.put(new PageKey(taskId.toString(), bufferId), new PageData(token, taskInstanceID));
                long start = System.nanoTime();
                log.info("Going to fetch pages from location =%s", bufferLocation);
                PageDataFetcher pageBackupClient = new PageDataFetcher(httpClient, bufferLocation, token);
                Iterator<List<SerializedPage>> pages = pageBackupClient.getPages();

                LinkedList<SerializedPage> serializedPages = new LinkedList<>();
                while (pages.hasNext()) {
                    List<SerializedPage> nextPages = pages.next();
                    if (!nextPages.isEmpty()) {
                        serializedPages.addAll(nextPages);
                    }
                }
                log.info("Page fetching is successful from location =%s", bufferLocation);
                pageBackupClient.close();
                log.info("Abort is successful to location =%s", bufferLocation);

                log.info("Going to update page data for task: %s , bufferId: %s", taskId, bufferId);
                PageData pageData = new PageData(token, taskInstanceID);
                pageData.updatePages(serializedPages);
                pageCache.put(new PageKey(taskId.toString(), bufferId), pageData);
                long duration = Duration.nanosSince(start).roundTo(TimeUnit.SECONDS);
                eventListenerManager.trackPreemptionLifeCycle(
                        taskId,
                        QueryRecoveryDebugInfo.builder()
                                .outputBufferID(bufferId)
                                .state(QueryRecoveryState.DATA_PAGE_TRANSFER_INIT_COMPLETED)
                                .extraInfo(ImmutableMap.of(
                                        "size", String.valueOf(serializedPages.size()),
                                        "duration", String.valueOf(duration)))
                                .build());
            }
            catch (Throwable ex) {
                log.error(ex, "Failed to fetch pages from location =%s", bufferLocation);
                eventListenerManager.trackPreemptionLifeCycle(
                        taskId,
                        QueryRecoveryDebugInfo.builder()
                                .outputBufferID(bufferId)
                                .state(QueryRecoveryState.DATA_PAGE_TRANSFER_INIT_FAILED)
                                .extraInfo(new ImmutableMap.Builder<String, String>().put("bufferLocation", String.valueOf(bufferLocation)).build())
                                .build());
            }
        });
    }

    public ListenableFuture<BufferResult> getTaskResultsFromCache(TaskId taskId, OutputBuffers.OutputBufferId bufferId, long sequenceId, DataSize maxSize)
    {
        PageKey pageKey = new PageKey(taskId.toString(), bufferId.toString());
        Optional<PageData> pageDataCache = getPageData(pageKey);
        if (!pageDataCache.isPresent()) {
            throw new PrestoException(PAGE_CACHE_UNAVAILABLE, String.format("page cache not available for task:%s, buffer:%s", taskId, bufferId));
        }
        PageData pageData = pageDataCache.get();
        //if pages are not present in the cache, download might not be started, return empty page and dont close the buffer
        if (!pageData.getPages().isPresent()) {
            return immediateFuture(emptyResults(pageData.getTaskInstanceID(), sequenceId, false));
        }
        // we have some pages to ack and wipe out pages
        pageData = acknowledgeCachedPage(taskId, bufferId, sequenceId);
        return immediateFuture(processRead(sequenceId, maxSize, pageData));
    }

    public PageData acknowledgeCachedPage(TaskId taskId, OutputBuffers.OutputBufferId bufferId, long sequenceId)
    {
        checkArgument(pageCache != null, "cache not available");
        PageKey pageKey = new PageKey(taskId.toString(), bufferId.toString());
        Optional<PageData> pageDataCache = getPageData(pageKey);
        if (!pageDataCache.isPresent()) {
            log.error("acknowledgeCachedPage::No pages found in cache for task %s, and buffer %s", taskId, bufferId);
            throw new RuntimeException("acknowledgeCachedPage::Data node does not have any pages for task");
        }
        PageData pageData = pageDataCache.get();
        log.info("acknowledgeCachedPage::Getting page data from cache for task %s, bufferId %s, sequenceId %s", taskId, bufferId, sequenceId);
        acknowledgeCachedPages(sequenceId, pageData);
        return pageData;
    }

    private Optional<PageData> getPageData(PageKey pageKey)
    {
        checkArgument(pageCache != null, "cache not available");
        PageData pageData = pageCache.getIfPresent(pageKey);
        return Optional.ofNullable(pageData);
    }

    private BufferResult processRead(long sequenceId, DataSize maxSize, PageData pageData)
    {
        // if request is for pages before the current position, just return an empty result
        if (sequenceId < pageData.getCurrentSequenceID()) {
            return emptyResults(pageData.getTaskInstanceID(), sequenceId, false);
        }
        if (pageData.getPages().get().isEmpty()) {
            log.info("Reached end of page, returning buffer complete");
            return emptyResults(pageData.getTaskInstanceID(), sequenceId, true);
        }

        // if request is for pages after the current position, there is a bug somewhere
        // a read call is always proceeded by acknowledge pages, which
        // will advance the sequence id to at least the request position, unless
        // the buffer is destroyed, and in that case the buffer will be empty with
        // no more pages set, which is checked above
        verify(sequenceId == pageData.getCurrentSequenceID(), "Invalid sequence id");

        // read the new pages
        long maxBytes = maxSize.toBytes();
        List<SerializedPage> result = new ArrayList<>();
        long bytes = 0;

        for (SerializedPage page : pageData.getPages().get()) {
            bytes += page.getRetainedSizeInBytes();
            // break (and don't add) if this page would exceed the limit
            if (!result.isEmpty() && bytes > maxBytes) {
                break;
            }
            result.add(page);
        }
        return new BufferResult(pageData.getTaskInstanceID(), sequenceId, sequenceId + result.size(), false, result);
    }

    public void acknowledgeCachedPages(long sequenceId, PageData pageData)
    {
        checkArgument(sequenceId >= 0, "acknowledgeCachedPage::Invalid sequence id");
        // if pages have already been acknowledged, just ignore this
        long oldCurrentSequenceId = pageData.getCurrentSequenceID();
        int pagesToRemove = toIntExact(sequenceId - oldCurrentSequenceId);
        checkArgument(pageData.getPages() != null, "Page data not present");
        LinkedList<SerializedPage> serializedPageReferences = pageData.getPages().get();
        checkArgument(pagesToRemove <= serializedPageReferences.size(), "Invalid sequence id");
        for (int i = 0; i < pagesToRemove; i++) {
            log.info("acknowledgeCachedPages::Removed page %s", i);
            serializedPageReferences.removeFirst();
        }
        if (sequenceId < oldCurrentSequenceId) {
            return;
        }
        log.info("acknowledgeCachedPages::set current sequence id = %s", oldCurrentSequenceId + pagesToRemove);
        pageData.setCurrentSequenceID(oldCurrentSequenceId + pagesToRemove);
    }

    public TaskInfo abortTaskResult(TaskId taskId, OutputBuffers.OutputBufferId bufferId)
    {
        log.info("acknowledgeTaskResults:: going to ack task result from data node");
        // wipe out the buffer, but what to return in task? why do we need task
        checkArgument(pageCache != null, "cache not available");
        pageCache.invalidate(new PageKey(taskId.toString(), bufferId.toString()));
        return null;
    }

    //FIXME hack
    public EventListenerManager getEventListenerManager()
    {
        return eventListenerManager;
    }
}
