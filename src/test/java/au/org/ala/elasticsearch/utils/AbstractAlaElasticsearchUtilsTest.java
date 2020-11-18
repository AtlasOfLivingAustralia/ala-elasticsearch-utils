package au.org.ala.elasticsearch.utils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.IndexTemplateMetadata;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
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

        AlaElasticsearchTestUtils.addSampleDocument(testESClient, testSourceIndex);

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