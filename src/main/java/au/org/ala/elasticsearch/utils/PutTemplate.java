/**
 *
 */
package au.org.ala.elasticsearch.utils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.elasticsearch.client.RestHighLevelClient;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Puts a template into elasticsearch so that it can be used for future indexes
 * or reindexing.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class PutTemplate {

    public static void main(String... args) throws Exception {
        final OptionParser parser = new OptionParser();

        final OptionSpec<Void> help = parser.accepts("help").forHelp();
        final OptionSpec<String> templateNameOption = parser.accepts("template-name")
                .withRequiredArg().ofType(String.class).required()
                .describedAs("The name of the template");
        final OptionSpec<File> templateJsonOption = parser.accepts("template-json")
                .withRequiredArg().ofType(File.class).required()
                .describedAs("The file containing the JSON for the template");
        final OptionSpec<String> esHostnameOption = parser.accepts("es-hostname").withRequiredArg()
                .ofType(String.class).defaultsTo("localhost")
                .describedAs("The hostname of the elasticsearch cluster.");
        final OptionSpec<Integer> esPortOption = parser.accepts("es-port").withRequiredArg()
                .ofType(Integer.class).defaultsTo(9200)
                .describedAs("The TCP port of the elasticsearch cluster.");
        final OptionSpec<String> esSchemeOption = parser.accepts("es-scheme").withRequiredArg()
                .ofType(String.class).defaultsTo("http").describedAs(
                        "The scheme (HTTP or HTTPS) to use to contact the elasticsearch cluster.");

        OptionSet options = null;

        try {
            options = parser.parse(args);
        } catch (final OptionException e) {
            System.out.println(e.getMessage());
            parser.printHelpOn(System.out);
            throw e;
        }

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            return;
        }

        final String templateName = templateNameOption.value(options);
        final Path templateJsonPath = templateJsonOption.value(options).toPath().normalize()
                .toAbsolutePath();
        final String templateContentJson = Files.readString(templateJsonPath,
                StandardCharsets.UTF_8);

        final String esHostname = esHostnameOption.value(options);
        final int esPort = esPortOption.value(options);
        final String esScheme = esSchemeOption.value(options);

        // Reference:
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-getting-started-initialization.html
        try (RestHighLevelClient client = AlaElasticsearchUtils.newElasticsearchClient(esHostname,
                esPort, esScheme);) {
            AlaElasticsearchUtils.putTemplate(client, templateName, templateContentJson);
        }
    }

}
