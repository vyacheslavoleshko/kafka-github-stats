package rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import model.ContributorWithCount;
import model.RepoStats;
import model.TopFiveContributors;
import serde.RepoStatsSerializer;
import serde.TopFiveSerializer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/")
public class GithubAnalyzerRestService {

    private final ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    @GET()
    @Path("/repository")
    @Produces(MediaType.TEXT_PLAIN)
    public String getRepositories() {
        return mapper.writeValueAsString(List.of("repo1", "repo2"));
    }

    @SneakyThrows
    @GET()
    @Path("/repository/stats")
    @Produces(MediaType.TEXT_PLAIN)
    public String getRepositoryStatistics(@PathParam("repo") String repositoryName) {
        RepoStats dummy = new RepoStats();
        var top = new TopFiveContributors();
        top.add(new ContributorWithCount("repow", "email@gmail", 1L));
        top.add(new ContributorWithCount("repow", "email2@gmail", 2L));
        dummy.setRepo("repow");
        dummy.setTotalCommitters(100);
        dummy.setTotalCommits(1000);
        dummy.setTopFiveContributors(top);
        return mapper.writeValueAsString(dummy);
    }
}
