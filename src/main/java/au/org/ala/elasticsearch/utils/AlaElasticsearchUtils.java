/**
 *
 */
package au.org.ala.elasticsearch.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesResponse;
import org.elasticsearch.client.indices.IndexTemplateMetadata;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for interacting with Elasticsearch using the REST API.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class AlaElasticsearchUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AlaElasticsearchUtils.class);

    /**
     * Wait for task to complete.
     *
     * @param client
     *            The {@link RestHighLevelClient} to use.
     * @param taskId
     *            The task id to find and wait for completion before returning
     * @throws InterruptedException
     *             If waiting was interrupted.
     * @throws IOException
     *             If communication with the server had an issue.
     */
    public static void waitForTask(final RestHighLevelClient client, final String taskId)
            throws InterruptedException, IOException {
        final org.elasticsearch.tasks.TaskId parentTaskId = new org.elasticsearch.tasks.TaskId(
                taskId);

        boolean subTasksRunning = true;
        while (subTasksRunning) {
            final ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            final ListTasksResponse listTasksResponse = client.tasks().list(listTasksRequest,
                    RequestOptions.DEFAULT);
            if (listTasksResponse.getTasks().isEmpty()) {
                subTasksRunning = false;
            } else {
                boolean foundMatchingRunningTask = false;
                for (final TaskInfo taskInfo : listTasksResponse.getTasks()) {
                    if (taskInfo.getTaskId().equals(parentTaskId)) {
                        LOG.debug("Matching task: {}", taskInfo);
                        foundMatchingRunningTask = true;
                    } else {
                        LOG.debug("Non-matching task: {}", taskInfo);
                    }
                }
                subTasksRunning = foundMatchingRunningTask;
            }
            if (subTasksRunning) {
                LOG.debug("Task still running, waiting again: {}", taskId);
                // Wait before checking again
                Thread.sleep(100);
            }
        }
    }

    /**
     * Create a simple {@link RestHighLevelClient} using the given
     * hostname/port/scheme combination.
     *
     * @param esHostname
     *            The hostname to use for the given {@link RestHighLevelClient}.
     * @param esPort
     *            The port to use for the given {@link RestHighLevelClient}.
     * @param esScheme
     *            The HTTP/HTTPS scheme to use for the given
     *            {@link RestHighLevelClient}.
     * @return A {@link RestHighLevelClient} configured using the given
     *         parameters.
     */
    public static RestHighLevelClient newElasticsearchClient(final String esHostname,
            final int esPort, final String esScheme) {
        final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(esHostname, esPort, esScheme)));
        return client;
    }

    /**
     * Deletes the given index.
     *
     * @param client
     *            The {@link RestHighLevelClient} to use.
     * @param indexToDelete
     *            The name of the index to delete.
     * @throws IOException
     *             If communication with the server had an issue.
     */
    public static void deleteIndex(final RestHighLevelClient client, final String indexToDelete)
            throws IOException {
        final DeleteIndexRequest deleteDestinationRequest = new DeleteIndexRequest(indexToDelete);
        client.indices().delete(deleteDestinationRequest, RequestOptions.DEFAULT);
    }

    /**
     * Get the list of indices from the cluster that is being accessed by the
     * given REST client.
     *
     * @param client
     *            The {@link RestHighLevelClient} to use.
     * @return A {@link Set} of {@link String}s representing the index names.
     * @throws InterruptedException
     *             If waiting was interrupted.
     * @throws IOException
     *             If communication with the server had an issue.
     */
    public static Set<String> listIndexes(final RestHighLevelClient client)
            throws IOException, InterruptedException {
        final int maxRetries = 6;

        final ClusterHealthResponse response = getClusterStatus(client, maxRetries);

        final Map<String, ClusterIndexHealth> indices = response.getIndices();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Cluster name: {}", response.getClusterName());
            LOG.trace("Cluster status: {}", response.getStatus());
            LOG.trace("Cluster unassigned shards: {}", response.getUnassignedShards());
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Found {} indexes", indices.size());
            for (final Entry<String, ClusterIndexHealth> nextIndex : indices.entrySet()) {
                System.out.println(nextIndex.getKey());
            }
        }
        return Collections.unmodifiableSet(indices.keySet());
    }

    /**
     * Attempts to get the cluster status for the given REST client and indexes.
     * Will retry the given number of times if the cluster status is not
     * {@link ClusterHealthStatus.YELLOW} when the request is initially made.
     *
     * @param client
     *            The {@link RestHighLevelClient} to use.
     * @param maxRetries
     *            The maximum number of times to retry in an attempt to get a
     *            {@link ClusterHealthStatus.YELLOW} result before failing.
     * @param indexNames
     *            An optional array of index names to focus on. If not present,
     *            all indexes will be returned.
     * @return A {@link ClusterHealthResponse} object that can be used to
     *         diagnose the cluster health.
     * @throws InterruptedException
     *             If waiting was interrupted.
     * @throws IOException
     *             If communication with the server had an issue.
     */
    public static ClusterHealthResponse getClusterStatus(final RestHighLevelClient client,
            final int maxRetries, final String... indexNames)
            throws IOException, InterruptedException {
        Optional<ClusterHealthResponse> response = Optional.empty();

        for (int retries = 0; retries <= maxRetries; retries++) {
            final ClusterHealthRequest request = new ClusterHealthRequest(indexNames);
            request.waitForStatus(ClusterHealthStatus.YELLOW);
            request.level(ClusterHealthRequest.Level.INDICES);
            // request.waitForActiveShards(ActiveShardCount.ALL);
            // request.timeout(TimeValue.timeValueSeconds(20));
            // request.masterNodeTimeout(TimeValue.timeValueSeconds(10));

            final ClusterHealthResponse nextResponse = client.cluster().health(request,
                    RequestOptions.DEFAULT);

            if (nextResponse.isTimedOut()) {
                LOG.warn("Cluster Health Request timed out on attempt {}", (retries + 1));
                Thread.sleep(100);
                continue;
            } else {
                response = Optional.of(nextResponse);
                break;
            }
        }

        if (response.isEmpty()) {
            LOG.error("Failed to get cluster status after {} attempts/", maxRetries);
            throw new IOException(
                    "Failed to get cluster status after " + maxRetries + " attempts.");
        }

        return response.get();
    }

    /**
     * Obtain detailed information about the indexes in the elasticsearch
     * cluster being accessed.
     *
     * @param client
     *            The {@link RestHighLevelClient} to use.
     * @param indexNames
     *            An optional array of index names to focus on. If not present,
     *            all indexes will be returned.
     * @return A {@link Map} containing the information about the indexes.
     * @throws InterruptedException
     *             If waiting was interrupted.
     * @throws IOException
     *             If communication with the server had an issue.
     */
    public static Map<String, ClusterIndexHealth> indexInfo(final RestHighLevelClient client,
            final String... indexNames) throws IOException, InterruptedException {
        final int maxRetries = 3;

        final ClusterHealthResponse response = getClusterStatus(client, maxRetries, indexNames);

        final Map<String, ClusterIndexHealth> indices = response.getIndices();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Found {} indices.", indices.size());

            for (final Entry<String, ClusterIndexHealth> nextIndex : indices.entrySet()) {
                LOG.debug("Number of active shards in {}={}", nextIndex.getKey(),
                        nextIndex.getValue().getActiveShards());
            }
        }

        return indices;
    }

    /**
     * Put the template into elasticsearch.
     *
     * @param client
     *            The {@link RestHighLevelClient} to use.
     * @param templateName
     *            The {@link String} representing the name of the template.
     * @param templateContentJson
     *            The JSON-encoded template, as a {@link String}.
     * @throws InterruptedException
     *             If waiting was interrupted.
     * @throws IOException
     *             If communication with the server had an issue.
     */
    public static void putTemplate(final RestHighLevelClient client, final String templateName,
            final String templateContentJson) throws IOException, InterruptedException {
        final PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest(templateName);
        putRequest.source(templateContentJson, XContentType.JSON);

        final AcknowledgedResponse putTemplateResponse = client.indices().putTemplate(putRequest,
                RequestOptions.DEFAULT);

        if (putTemplateResponse.isAcknowledged()) {
            LOG.info("Put template request for {} was acknowledged", templateName);
        } else {
            LOG.error("Put template request for {} was not acknowledged", templateName);
        }
    }

    /**
     * Gets templates using the given template name as a reference.
     *
     * @param client
     *            The {@link RestHighLevelClient} to use.
     * @param templateName
     *            The {@link String} representing the name of the template.
     * @return A {@link List} of {@link IndexTemplateMetadata} objects matching
     *         the given template name.
     * @throws InterruptedException
     *             If waiting was interrupted.
     * @throws IOException
     *             If communication with the server had an issue.
     */
    public static List<IndexTemplateMetadata> getTemplate(final RestHighLevelClient client,
            final String templateName) throws IOException, InterruptedException {
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-get-templates.html
        final GetIndexTemplatesRequest getRequest = new GetIndexTemplatesRequest(templateName);

        final GetIndexTemplatesResponse getTemplatesResponse = client.indices()
                .getIndexTemplate(getRequest, RequestOptions.DEFAULT);

        final List<IndexTemplateMetadata> indexTemplates = getTemplatesResponse.getIndexTemplates();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Found {} existing templates matching {}.", indexTemplates.size(),
                    templateName);
            for (final IndexTemplateMetadata nextIndexTemplate : indexTemplates) {
                LOG.debug("Found existing index template named: {}", nextIndexTemplate.name());
            }
        }
        return indexTemplates;
    }

    public static void getFieldMappings(final RestHighLevelClient client,
            final String... indexNames) throws IOException, InterruptedException {
        final GetFieldMappingsRequest getRequest = new GetFieldMappingsRequest();
        getRequest.indices(indexNames);

    }

    public static Map<String, Map<String, FieldCapabilities>> getFieldCapabilities(
            final RestHighLevelClient client, final String... indexNames)
            throws IOException, InterruptedException {
        final FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices(indexNames);
        request.fields("*");
        request.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        final FieldCapabilitiesResponse fieldCapabilitiesResponse = client.fieldCaps(request,
                RequestOptions.DEFAULT);

        return fieldCapabilitiesResponse.get();
    }

    public static Map<String, Map<String, FieldCapabilities>> getUserDefinedFieldCapabilities(
            final RestHighLevelClient client, final String... indexNames)
            throws IOException, InterruptedException {
        return getFieldCapabilities(client, indexNames).entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith("_"))
                .collect(Collectors.toUnmodifiableMap(e -> e.getKey(), e -> e.getValue()));
    }

    public static Optional<Map<String, Object>> getDocumentByID(final RestHighLevelClient client,
            final String documentID, final String indexName)
            throws IOException, InterruptedException {

        final GetRequest getRequest = new GetRequest(indexName, documentID);
        final FetchSourceContext context = FetchSourceContext.FETCH_SOURCE;
        getRequest.fetchSourceContext(context);
        final GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        final String index = getResponse.getIndex();
        final String id = getResponse.getId();

        final Map<String, Object> sourceDocument = getResponse.getSourceAsMap();

        return Optional.ofNullable(sourceDocument);

    }

    public static SearchResponse search(final RestHighLevelClient client,
            final String... indexNames) throws IOException, InterruptedException {
        // SearchRequest searchRequest = new SearchRequest(indexNames);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexNames);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-query-builders.html
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }
}
