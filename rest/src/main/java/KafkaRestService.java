//import org.apache.kafka.common.serialization.LongSerializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StoreQueryParameters;
//import org.apache.kafka.streams.state.HostInfo;
//import org.apache.kafka.streams.state.QueryableStoreTypes;
//import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
//import org.eclipse.jetty.server.Server;
//import org.eclipse.jetty.server.ServerConnector;
//import org.eclipse.jetty.servlet.ServletContextHandler;
//import org.eclipse.jetty.servlet.ServletHolder;
//import org.glassfish.jersey.jackson.JacksonFeature;
//import org.glassfish.jersey.server.ResourceConfig;
//import org.glassfish.jersey.servlet.ServletContainer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.ws.rs.GET;
//import javax.ws.rs.NotFoundException;
//import javax.ws.rs.Path;
//import javax.ws.rs.PathParam;
//import javax.ws.rs.Produces;
//import javax.ws.rs.client.Client;
//import javax.ws.rs.client.ClientBuilder;
//import javax.ws.rs.core.GenericType;
//import javax.ws.rs.core.MediaType;
//import java.util.ArrayList;
//import java.util.List;

public class KafkaRestService {

//    private Server jettyServer;
//
//    /**
//     * Start an embedded Jetty Server
//     * @throws Exception from jetty
//     */
//    void start() throws Exception {
//        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
//        context.setContextPath("/");
//
//        jettyServer = new Server();
//        jettyServer.setHandler(context);
//
//        final ResourceConfig rc = new ResourceConfig();
//        rc.register(this);
//        rc.register(JacksonFeature.class);
//
//        final ServletContainer sc = new ServletContainer(rc);
//        final ServletHolder holder = new ServletHolder(ServletContainer.class.getCanonicalName());
//        context.addServlet(holder, "/*");
//
//        final ServerConnector connector = new ServerConnector(jettyServer);
//        connector.setHost(hostInfo.host());
//        connector.setPort(hostInfo.port());
//        jettyServer.addConnector(connector);
//
//        context.start();
//
//        try {
//            jettyServer.start();
//        } catch (final java.net.SocketException exception) {
//            log.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
//            throw new Exception(exception.toString());
//        }
//    }
//
//    /**
//     * Stop the Jetty Server
//     * @throws Exception from jetty
//     */
//    void stop() throws Exception {
//        if (jettyServer != null) {
//            jettyServer.stop();
//        }
//    }

}
