package au.org.ala.elasticsearch.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;

class ReindexTest extends AbstractAlaElasticsearchUtilsTest {

    /**
     * Test method for
     * {@link au.org.ala.elasticsearch.utils.Reindex#doReindex(RestHighLevelClient, String, String)}.
     */
    @Test
    final void testReindexMain() throws Exception {
        Reindex.main("--source", testSourceIndex, "--destination", testDestinationIndex);
    }

    /**
     * Test method for
     * {@link au.org.ala.elasticsearch.utils.Reindex#doReindex(RestHighLevelClient, String, String)}.
     */
    @Test
    final void testReindexDoReindex() throws Exception {
        Reindex.doReindex(testESClient, testSourceIndex, testDestinationIndex, testReindexScript);
    }

    /**
     * Test method for
     * {@link au.org.ala.elasticsearch.utils.Reindex#asyncReindex(org.elasticsearch.client.RestHighLevelClient, java.lang.String, java.lang.String)}.
     */
    @Test
    final void testAsyncReindex() throws Exception {
        final Optional<Map<String, Object>> sourceDocument = AlaElasticsearchUtils
                .getDocumentByID(testESClient, testDocumentID, testSourceIndex);

        assertNotNull(sourceDocument);

        assertTrue(sourceDocument.isPresent());

        final String reindexTaskId = Reindex.asyncReindex(testESClient, testSourceIndex,
                testDestinationIndex, testReindexScript);
        AlaElasticsearchUtils.waitForTask(testESClient, reindexTaskId);
        AlaElasticsearchUtils.refresh(testESClient, testSourceIndex, testDestinationIndex);

        Thread.sleep(1000);

        final Optional<Map<String, Object>> resultDocument = AlaElasticsearchUtils
                .getDocumentByID(testESClient, testDocumentID, testDestinationIndex);

        assertNotNull(resultDocument);

        assertTrue(resultDocument.isPresent());

        assertEquals(3, resultDocument.get().size());
        assertTrue(resultDocument.get().containsKey("message"));
        assertTrue(resultDocument.get().containsKey("postDate"));
        assertTrue(resultDocument.get().containsKey("postTime"));

        final SearchResponse searchResponse = AlaElasticsearchUtils.search(testESClient,
                testDestinationIndex);

        assertFalse(searchResponse.isTimedOut());
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertEquals(testDocumentID, searchResponse.getHits().iterator().next().getId());
        assertEquals(testDestinationIndex, searchResponse.getHits().iterator().next().getIndex());
    }

}
