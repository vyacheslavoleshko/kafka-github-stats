package rest;

import model.RepoStats;
import org.apache.kafka.common.protocol.types.Field;

import java.util.List;

public interface GithubAnalyzerApi {

    List<String> getRepositories();

    RepoStats getRepositoryStatistics(String repositoryName);

}
