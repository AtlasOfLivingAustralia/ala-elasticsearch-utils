package au.org.ala.elasticsearch.utils;

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
        Reindex.doReindex(testESClient, testSourceIndex, testDestinationIndex);
    }

    /**
     * Test method for
     * {@link au.org.ala.elasticsearch.utils.Reindex#asyncReindex(org.elasticsearch.client.RestHighLevelClient, java.lang.String, java.lang.String)}.
     */
    @Test
    final void testAsyncReindex() throws Exception {
        final String reindexTaskId = Reindex.asyncReindex(testESClient, testSourceIndex,
                testDestinationIndex);
        AlaElasticsearchUtils.waitForTask(testESClient, reindexTaskId);
    }

}
