/**
 *
 */
package au.org.ala.elasticsearch.utils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Utilities for testing.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
class AlaElasticsearchTestUtils {

    /**
     * @param client
     * @param sourceIndex
     * @param destinationIndex
     * @throws IOException
     */
    public static void deleteAndRecreateIndexes(RestHighLevelClient client,
            final String sourceIndex, final String destinationIndex) throws IOException {
        // Reference:
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-indices-exists.html
        final GetIndexRequest getSourceRequest = new GetIndexRequest(sourceIndex);

        if (client.indices().exists(getSourceRequest, RequestOptions.DEFAULT)) {
            AlaElasticsearchUtils.deleteIndex(client, sourceIndex);
        }

        final GetIndexRequest getDestinationRequest = new GetIndexRequest(destinationIndex);

        if (client.indices().exists(getDestinationRequest, RequestOptions.DEFAULT)) {
            AlaElasticsearchUtils.deleteIndex(client, destinationIndex);
        }

        // Reference:
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-create-index.html
        final CreateIndexRequest createSourceRequest = new CreateIndexRequest(sourceIndex);

        createSourceRequest.settings(Settings.builder().put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0));

        // createSourceRequest.mapping(
        // "{\"properties\": { \"message\": { \"type\": \"text\" },
        // \"postDate\": { \"type\": \"text\" } } }",
        // XContentType.JSON);

        final CreateIndexResponse createSourceIndexResponse = client.indices()
                .create(createSourceRequest, RequestOptions.DEFAULT);
        System.out.println(
                String.format("Created source index. acknowledged=%s shardsAcknowledged=%s",
                        createSourceIndexResponse.isAcknowledged(),
                        createSourceIndexResponse.isShardsAcknowledged()));

        // Reference:
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-create-index.html
        final CreateIndexRequest createDestinationRequest = new CreateIndexRequest(
                destinationIndex);

        createDestinationRequest.settings(Settings.builder().put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0));

        // Example: Change the postDate type to date
        // createDestinationRequest.mapping(
        // "{\"properties\": { \"message\": { \"type\": \"text\" },
        // \"postDate\": { \"type\": \"date\" } } }",
        // XContentType.JSON);

        final CreateIndexResponse createDestinationIndexResponse = client.indices()
                .create(createDestinationRequest, RequestOptions.DEFAULT);
        System.out.println(
                String.format("Created destination index. acknowledged=%s shardsAcknowledged=%s",
                        createDestinationIndexResponse.isAcknowledged(),
                        createDestinationIndexResponse.isShardsAcknowledged()));
    }

    public static IndexResponse addSampleDocument(RestHighLevelClient client, String indexName,
            String documentID) throws IOException, InterruptedException {
        final String currentDateTime = DateTimeFormatter.ISO_LOCAL_DATE_TIME
                .format(LocalDateTime.now()) + "Z";
        final String jsonString = "{ \"postDate\": \"" + currentDateTime
                + "\", \"message\": \"Testing reindex process\" }";
        System.out.println(jsonString);

        final IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.id(documentID);
        indexRequest.source(jsonString, XContentType.JSON);
        indexRequest.create(true);
        indexRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        final IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println("indexResponse.getId()=" + indexResponse.getId());
        System.out.println("indexResponse=" + indexResponse);

        return indexResponse;
    }

}
