package rest;

import com.fasterxml.jackson.core.util.JacksonFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import model.RepoStats;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;

public class KafkaRestService implements GithubAnalyzerApi {

    private static final Logger log = Logger.getLogger(KafkaRestService.class.getSimpleName());

    private final KafkaStreams streams;
    private final String thisAppHost;
    private final int thisAppPort;

    public KafkaRestService(KafkaStreams streams, String thisAppHost, int thisAppPort) {
        this.streams = streams;
        this.thisAppHost = thisAppHost;
        this.thisAppPort = thisAppPort;
    }

    @Override
    public List<String> getRepositories() {
        return List.of();
    }

    @Override
    public RepoStats getRepositoryStatistics(String repositoryName) {
        return null;
    }

    private Server jettyServer;

    /**
     * Start an embedded Jetty Server
     * @throws Exception from jetty
     */
    void start() throws Exception {
        URI baseUri = UriBuilder.fromUri(thisAppHost).port(thisAppPort).build();
        ResourceConfig config = new ResourceConfig();
        config.register(GithubAnalyzerRestService.class);
        config.register(JacksonJaxbJsonProvider.class);
        jettyServer = JettyHttpContainerFactory.createServer(baseUri, config);
        try {
            jettyServer.start();
            jettyServer.join();
        } catch (final java.net.SocketException exception) {
            log.severe("Unavailable: " + thisAppHost + ":" + thisAppPort);
            throw new Exception(exception.toString());
        }
    }

    /**
     * Stop the Jetty Server
     * @throws Exception from jetty
     */
    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        p.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "github-analyzer");
        p.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        p.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> commits = builder.stream("github.commits");

        var service = new KafkaRestService(
                new KafkaStreams(builder.build(), p), "http://127.0.0.1", 8089
        );
        service.start();
        System.out.println(service.jettyServer.isStarted());
    }

}
