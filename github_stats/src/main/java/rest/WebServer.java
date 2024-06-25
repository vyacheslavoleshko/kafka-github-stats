package rest;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.jetty.server.Server;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.logging.Logger;

public class WebServer {

    private static final Logger log = Logger.getLogger(WebServer.class.getSimpleName());

    private final String host;
    private final int port;
    private Server jettyServer;

    public WebServer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Start an embedded Jetty Server
     * @throws Exception from jetty
     */
    public void startWebServer(ResourceConfig config) throws Exception {
        URI baseUri = UriBuilder.fromUri(host).port(port).build();

        jettyServer = JettyHttpContainerFactory.createServer(baseUri, config);
        try {
            jettyServer.start();
            jettyServer.join();
        } catch (final java.net.SocketException exception) {
            log.severe("Unavailable: " + host + ":" + port);
            throw new Exception(exception.toString());
        }
    }

    /**
     * Stop the Jetty Server
     * @throws Exception from jetty
     */
    public void stopWebServer() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
            jettyServer.join();
        }
    }
}
