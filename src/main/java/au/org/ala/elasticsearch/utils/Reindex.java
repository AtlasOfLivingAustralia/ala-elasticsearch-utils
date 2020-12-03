/**
 *
 */
package au.org.ala.elasticsearch.utils;

import java.io.IOException;
import java.util.Collections;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class Reindex {

    private static final Logger LOG = LoggerFactory.getLogger(Reindex.class);

    public static void main(String... args) throws Exception {
        final OptionParser parser = new OptionParser();

        final OptionSpec<Void> help = parser.accepts("help").forHelp();
        final OptionSpec<String> sourceOption = parser.accepts("source").withRequiredArg()
                .ofType(String.class).required()
                .describedAs("The elasticsearch index to use as the source for the reindex");
        final OptionSpec<String> destinationOption = parser.accepts("destination").withRequiredArg()
                .ofType(String.class).required()
                .describedAs("The elasticsearch index to use as the destination for the reindex");
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

        final String sourceIndex = sourceOption.value(options);
        final String destinationIndex = destinationOption.value(options);

        final String esHostname = esHostnameOption.value(options);
        final int esPort = esPortOption.value(options);
        final String esScheme = esSchemeOption.value(options);

        final Script script = new Script(ScriptType.INLINE, "painless",
                "if (ctx. == '') {ctx._source.likes++;}", Collections.emptyMap());

        // Reference:
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-getting-started-initialization.html
        try (RestHighLevelClient client = AlaElasticsearchUtils.newElasticsearchClient(esHostname,
                esPort, esScheme);) {
            doReindex(client, sourceIndex, destinationIndex, script);
        }
    }

    /**
     * Reindex the given source index to the given destination index.
     *
     * @param client
     *            The Elasticsearch {@link RestHighLevelClient} to use to
     *            connect to the elasticsearch cluster.
     * @param sourceIndex
     *            The source for the reindex.
     * @param destinationIndex
     *            The destination for the reindex.
     * @param script
     * @throws IOException
     *             If communication with the server had an issue.
     * @throws InterruptedException
     *             If waiting was interrupted.
     */
    public static void doReindex(RestHighLevelClient client, String sourceIndex,
            String destinationIndex, Script script) throws IOException, InterruptedException {
        final String taskId = Reindex.asyncReindex(client, sourceIndex, destinationIndex, script);

        AlaElasticsearchUtils.waitForTask(client, taskId);
    }

    /**
     * Asynchronously trigger a reindex of the given source index to the given
     * destination index.
     *
     * @param client
     *            The {@link RestHighLevelClient} to use.
     * @param sourceIndex
     *            The source index.
     * @param destinationIndex
     *            The destination index.
     * @param script
     *            The script to use to translate documents that are being
     *            reindexed.
     * @return The task ID of the reindex task that was asynchronously run.
     * @throws IOException
     *             If communication with the server had an issue.
     */
    public static String asyncReindex(final RestHighLevelClient client, final String sourceIndex,
            final String destinationIndex, Script script) throws IOException {
        // Reference:
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-document-reindex.html
        final ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndex);
        reindexRequest.setDestIndex(destinationIndex);
        reindexRequest.setDestVersionType(VersionType.EXTERNAL);
        reindexRequest.setDestOpType("index");
        reindexRequest.setRefresh(true);

        reindexRequest.setScript(script);

        // TODO: Very small fixed sample size while creating the algorithms
        // reindexRequest.setMaxDocs(10);

        final TaskSubmissionResponse reindexSubmission = client.submitReindexTask(reindexRequest,
                RequestOptions.DEFAULT);
        final String taskId = reindexSubmission.getTask();
        LOG.debug("Task created with id: {}", taskId);
        return taskId;
    }

}
