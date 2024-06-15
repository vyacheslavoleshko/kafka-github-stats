package rest;

import com.fasterxml.jackson.core.util.JacksonFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import lombok.SneakyThrows;
import model.RepoStats;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.inject.Inject;
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
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

@Path("/")
public class GithubAnalyzerRestService {

    private static final Logger log = Logger.getLogger(GithubAnalyzerRestService.class.getSimpleName());
    private final ObjectMapper mapper = new ObjectMapper();

    private KafkaRestService restService;

    @Inject
    public GithubAnalyzerRestService(KafkaRestService restService) {
        this.restService = restService;
    }

    @SneakyThrows
    @GET()
    @Path("/repository")
    @Produces(MediaType.TEXT_PLAIN)
    public String getRepositoriesApi() {
        return mapper.writeValueAsString(restService.getRepositories());
    }

    @SneakyThrows
    @GET()
    @Path("/repository/stats/{repo}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getRepositoryStatisticsApi(@PathParam("repo") String repositoryName) {
        return mapper.writeValueAsString(restService.getRepositoryStatistics(repositoryName));
    }

}