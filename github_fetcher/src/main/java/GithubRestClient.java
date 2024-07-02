import lombok.SneakyThrows;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHCommitQueryBuilder;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.PagedIterator;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Rest client that uses org.kohsuke.github to access GitHub API.
 */
public class GithubRestClient implements GithubRestClientApi {

    private static final Logger log = Logger.getLogger(GithubRestClient.class.getSimpleName());

    private final GitHub github;
    private static GithubRestClient INSTANCE = null;

    private static final ExecutorService commitExecutorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);

    GithubRestClient(GitHub github) {
        this.github = github;
    }

    public static GithubRestClientApi getInstance(String githubToken) {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        try {
            INSTANCE = new GithubRestClient(
                    new GitHubBuilder()
                            .withOAuthToken(githubToken)
                            .build());
            return INSTANCE;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SneakyThrows
    public void processCommitsInBatches(GithubFetcher.FetchRequest request, int batchSize, Consumer<List<GithubFetcher.Commit>> processor) {
        var iterator = getCommits(request.getOwner(), request.getRepo(), request.getSince(), batchSize);
        while (iterator.hasNext()) {
            List<GithubFetcher.Commit> commits = mapCommits(iterator.nextPage(), request);
            log.info(String.format("%s_%s: Fetched %s commits", request.getOwner(), request.getRepo(), commits.size()));
            processor.accept(commits);
            log.info(String.format("%s_%s: Processed %s messages", request.getOwner(), request.getRepo(), commits.size()));
        }
    }

    PagedIterator<GHCommit> getCommits(String owner, String repo, LocalDate since, int batchSize) throws IOException {
        GHRepository repository = github.getRepository(owner + "/" + repo);
        Calendar cal = Calendar.getInstance();
        cal.set(since.getYear(), since.getMonthValue() - 1, since.getDayOfMonth());
        Date sinceDate = cal.getTime();
        GHCommitQueryBuilder queryBuilder = repository.queryCommits().since(sinceDate);
        return queryBuilder.list().withPageSize(batchSize).iterator();
    }

    private List<GithubFetcher.Commit> mapCommits(List<GHCommit> commits, GithubFetcher.FetchRequest req) {
        CompletableFuture<GithubFetcher.Commit>[] futures = commits.stream()
                .map(commit -> CompletableFuture.supplyAsync(
                        () -> fetchCommitDetails(req.getOwner(), req.getRepo(), commit, req.getId()), commitExecutorService))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();

        List<GithubFetcher.Commit> result = new ArrayList<>();
        for (CompletableFuture<GithubFetcher.Commit> future : futures) {
            result.add(future.join());
        }
        return result;
    }

    @SneakyThrows
    private GithubFetcher.Commit fetchCommitDetails(String owner, String repo, GHCommit commit, UUID fetchRequestId) {
        GHUser author = commit.getAuthor();
        String name = "unknown" + UUID.randomUUID();
        if (author != null) name = author.getLogin();
        return new GithubFetcher.Commit(owner + ":" + repo, name, fetchRequestId);
    }

}
