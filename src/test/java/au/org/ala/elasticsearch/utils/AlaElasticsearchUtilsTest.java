/**
 *
 */
package au.org.ala.elasticsearch.utils;

import java.util.Map;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AlaElasticsearchUtils}.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
class AlaElasticsearchUtilsTest extends AbstractAlaElasticsearchUtilsTest {

    @Test
    public void testGetFieldCapabilities() throws Exception {
        final Map<String, Map<String, FieldCapabilities>> sourceFieldCapabilities = AlaElasticsearchUtils
                .getFieldCapabilities(testESClient, testSourceIndex);

        Assertions.assertEquals(13, sourceFieldCapabilities.size());

        final Map<String, Map<String, FieldCapabilities>> destinationFieldCapabilitiesBefore = AlaElasticsearchUtils
                .getFieldCapabilities(testESClient, testDestinationIndex);

        Assertions.assertEquals(14, destinationFieldCapabilitiesBefore.size());

        Reindex.doReindex(testESClient, testSourceIndex, testDestinationIndex, testReindexScript);

        final Map<String, Map<String, FieldCapabilities>> destinationFieldCapabilitiesAfter = AlaElasticsearchUtils
                .getFieldCapabilities(testESClient, testDestinationIndex);

        Assertions.assertEquals(14, destinationFieldCapabilitiesAfter.size());

    }

    @Test
    public void testGetUserDefinedFieldCapabilities() throws Exception {
        final Map<String, Map<String, FieldCapabilities>> sourceFieldCapabilities = AlaElasticsearchUtils
                .getUserDefinedFieldCapabilities(testESClient, testSourceIndex);

        Assertions.assertEquals(2, sourceFieldCapabilities.size());
        Assertions.assertTrue(sourceFieldCapabilities.containsKey("message"));
        Assertions.assertEquals("message",
                sourceFieldCapabilities.get("message").get("text").getName());
        Assertions.assertEquals("text",
                sourceFieldCapabilities.get("message").get("text").getType());
        Assertions.assertTrue(sourceFieldCapabilities.containsKey("postDate"));
        Assertions.assertTrue(sourceFieldCapabilities.get("postDate").containsKey("text"));
        Assertions.assertEquals("postDate",
                sourceFieldCapabilities.get("postDate").get("text").getName());
        Assertions.assertEquals("text",
                sourceFieldCapabilities.get("postDate").get("text").getType());

        final Map<String, Map<String, FieldCapabilities>> destinationFieldCapabilitiesBefore = AlaElasticsearchUtils
                .getUserDefinedFieldCapabilities(testESClient, testDestinationIndex);

        Assertions.assertEquals(3, destinationFieldCapabilitiesBefore.size());

        Reindex.doReindex(testESClient, testSourceIndex, testDestinationIndex, testReindexScript);
        Thread.sleep(1000);

        final Map<String, Map<String, FieldCapabilities>> destinationFieldCapabilitiesAfter = AlaElasticsearchUtils
                .getUserDefinedFieldCapabilities(testESClient, testDestinationIndex);

        Assertions.assertEquals(3, destinationFieldCapabilitiesAfter.size());
        Assertions.assertTrue(destinationFieldCapabilitiesAfter.containsKey("message"));
        Assertions.assertEquals("message",
                destinationFieldCapabilitiesAfter.get("message").get("text").getName());
        Assertions.assertEquals("text",
                destinationFieldCapabilitiesAfter.get("message").get("text").getType());

        Assertions.assertTrue(destinationFieldCapabilitiesAfter.containsKey("postDate"));
        Assertions
                .assertTrue(destinationFieldCapabilitiesAfter.get("postDate").containsKey("date"));
        Assertions.assertEquals("postDate",
                destinationFieldCapabilitiesAfter.get("postDate").get("date").getName());
        Assertions.assertEquals("date",
                destinationFieldCapabilitiesAfter.get("postDate").get("date").getType());

        Assertions.assertTrue(destinationFieldCapabilitiesAfter.containsKey("postTime"));
        Assertions
                .assertTrue(destinationFieldCapabilitiesAfter.get("postTime").containsKey("date"));
        Assertions.assertEquals("postTime",
                destinationFieldCapabilitiesAfter.get("postTime").get("date").getName());
        Assertions.assertEquals("date",
                destinationFieldCapabilitiesAfter.get("postTime").get("date").getType());
    }

}
