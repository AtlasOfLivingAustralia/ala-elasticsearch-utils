/**
 *
 */
package au.org.ala.elasticsearch.utils;

import org.junit.jupiter.api.Test;

/**
 * Test for {@link PutTemplate}.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
class PutTemplateTest extends AbstractAlaElasticsearchUtilsTest {

    /**
     * Test method for
     * {@link au.org.ala.elasticsearch.utils.PutTemplate#main(java.lang.String[])}.
     */
    @Test
    final void testPutTemplateMain() throws Exception {
        PutTemplate.main("--template-name", testSourceIndexTemplateName, "--template-json",
                sourceTemplateTempFile.toAbsolutePath().toString());

    }

}
