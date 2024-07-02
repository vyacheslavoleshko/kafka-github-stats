package rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.logging.Logger;

/**
 * Controllers to be used by the Frontend.
 */
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