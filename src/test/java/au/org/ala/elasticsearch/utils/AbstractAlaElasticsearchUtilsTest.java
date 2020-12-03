package au.org.ala.elasticsearch.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.IndexTemplateMetadata;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Abstract superclass for tests.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
abstract class AbstractAlaElasticsearchUtilsTest {

    protected static RestHighLevelClient testESClient;
    protected static String testSourceIndex = "example-source-index-utils-test";
    protected static String testSourceIndexTemplateName = "index-template-source-test-1";
    protected static String testSourceIndexTemplate = "/au/org/ala/elasticsearch/utils/test/index-template-source-1.json";
    protected static String testSourceIndexTemplateJSON;
    protected static String testDestinationIndex = "example-destination-index-utils-test";
    protected static String testDestinationIndexTemplateName = "index-template-destination-test-1";
    protected static String testDestinationIndexTemplate = "/au/org/ala/elasticsearch/utils/test/index-template-destination-1.json";
    protected static String testDestinationIndexTemplateJSON;

    @TempDir
    protected static Path testDir;

    protected static Path sourceTemplateTempFile;
    protected static Path destinationTemplateTempFile;

    protected static Script testReindexScript;

    protected static String testDocumentID;

    /**
     * @throws java.lang.Exception
     */
    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        final String esHostname = "localhost";
        final int esPort = 9200;
        final String esScheme = "http";

        testESClient = AlaElasticsearchUtils.newElasticsearchClient(esHostname, esPort, esScheme);

        sourceTemplateTempFile = testDir.resolve("test-source-template.json");
        Files.copy(AlaElasticsearchUtils.class.getResourceAsStream(testSourceIndexTemplate),
                sourceTemplateTempFile);
        testSourceIndexTemplateJSON = Files.readString(sourceTemplateTempFile,
                StandardCharsets.UTF_8);
        // System.out.println(testSourceIndexTemplateJSON);

        destinationTemplateTempFile = testDir.resolve("test-destination-template.json");
        Files.copy(AlaElasticsearchUtils.class.getResourceAsStream(testDestinationIndexTemplate),
                destinationTemplateTempFile);
        testDestinationIndexTemplateJSON = Files.readString(destinationTemplateTempFile,
                StandardCharsets.UTF_8);
        // System.out.println(testDestinationIndexTemplateJSON);

        // https://www.elastic.co/guide/en/elasticsearch/reference/master/modules-scripting-painless.html
        // https://www.elastic.co/guide/en/elasticsearch/painless/master/painless-datetime.html

        final ZonedDateTime zonedDateTime = ZonedDateTime.parse("2020-12-03T13:19:00.002Z");
        final String localDate = DateTimeFormatter.ISO_LOCAL_DATE.format(zonedDateTime);
        final String localTime = DateTimeFormatter.ISO_LOCAL_TIME.format(zonedDateTime);
        System.out.println("localDate=" + localDate);
        System.out.println("localTime=" + localTime);

        testReindexScript = new Script(ScriptType.INLINE, "painless",
                "if (ctx.postDate != '') {ZonedDateTime zdt = ZonedDateTime.parse(ctx.postDate); ctx.postDate = DateTimeFormatter.ISO_LOCAL_DATE.format(zdt); ctx.postTime.DateTimeFormatter.ISO_LOCAL_TIME.format(zdt);}",
                Collections.emptyMap());

        testDocumentID = "1";
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterAll
    static void tearDownAfterClass() throws Exception {
        testESClient.close();
    }

    public AbstractAlaElasticsearchUtilsTest() {
        super();
    }

    /**
     * @throws java.lang.Exception
     */
    @BeforeEach
    void setUp() throws Exception {
        AlaElasticsearchUtils.putTemplate(testESClient, testSourceIndexTemplateName,
                testSourceIndexTemplateJSON);
        AlaElasticsearchUtils.putTemplate(testESClient, testDestinationIndexTemplateName,
                testDestinationIndexTemplateJSON);

        final List<IndexTemplateMetadata> sourceIndexTemplates = AlaElasticsearchUtils
                .getTemplate(testESClient, testSourceIndexTemplateName);
        Assertions.assertEquals(1, sourceIndexTemplates.size());
        Assertions.assertEquals(testSourceIndexTemplateName, sourceIndexTemplates.get(0).name());
        final List<IndexTemplateMetadata> destinationIndexTemplates = AlaElasticsearchUtils
                .getTemplate(testESClient, testDestinationIndexTemplateName);
        Assertions.assertEquals(1, destinationIndexTemplates.size());
        Assertions.assertEquals(testDestinationIndexTemplateName,
                destinationIndexTemplates.get(0).name());

        AlaElasticsearchTestUtils.deleteAndRecreateIndexes(testESClient, testSourceIndex,
                testDestinationIndex);

        AlaElasticsearchTestUtils.addSampleDocument(testESClient, testSourceIndex, testDocumentID);

        // Thread.sleep(10000);

        final Set<String> testIndexes = AlaElasticsearchUtils.listIndexes(testESClient);
        Assertions.assertTrue(testIndexes.size() >= 2,
                "Less indexes than expected: " + testIndexes);

        final Map<String, ClusterIndexHealth> testSourceIndexInfo = AlaElasticsearchUtils
                .indexInfo(testESClient, testSourceIndex);
        Assertions.assertEquals(1, testSourceIndexInfo.size());
        Assertions.assertEquals(testSourceIndex, testSourceIndexInfo.keySet().iterator().next());
        Assertions.assertEquals(testSourceIndex,
                testSourceIndexInfo.values().iterator().next().getIndex());

        final Map<String, ClusterIndexHealth> testDestinationIndexInfo = AlaElasticsearchUtils
                .indexInfo(testESClient, testDestinationIndex);
        Assertions.assertEquals(1, testDestinationIndexInfo.size());
        Assertions.assertEquals(testDestinationIndex,
                testDestinationIndexInfo.keySet().iterator().next());
        Assertions.assertEquals(testDestinationIndex,
                testDestinationIndexInfo.values().iterator().next().getIndex());

        final SearchResponse searchResponse = AlaElasticsearchUtils.search(testESClient,
                testSourceIndex);

        assertFalse(searchResponse.isTimedOut());
        assertTrue(searchResponse.getHits().getTotalHits().value > 0);
        assertEquals(1L, searchResponse.getHits().getTotalHits().value);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterEach
    void tearDown() throws Exception {
        try {
            AlaElasticsearchUtils.deleteIndex(testESClient, testSourceIndex);
        } finally {
            AlaElasticsearchUtils.deleteIndex(testESClient, testDestinationIndex);
        }
    }

}