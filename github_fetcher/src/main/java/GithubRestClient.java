import org.apache.kafka.common.protocol.types.Field;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHCommitQueryBuilder;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.PagedIterable;
import org.kohsuke.github.PagedIterator;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GithubRestClient {

    private final GitHub github;

    public GithubRestClient(GitHub github) {
        this.github = github;
    }

    public PagedIterator<GHCommit> getCommits(String owner, String repo, LocalDate since) throws IOException {
        GHRepository repository = github.getRepository(owner + "/" + repo);
        Calendar cal = Calendar.getInstance();
        cal.set(since.getYear(), since.getMonthValue() - 1, since.getDayOfMonth());
        Date sinceDate = cal.getTime();
        GHCommitQueryBuilder queryBuilder = repository.queryCommits().since(sinceDate);
        return queryBuilder.list().withPageSize(50).iterator();
    }

}
