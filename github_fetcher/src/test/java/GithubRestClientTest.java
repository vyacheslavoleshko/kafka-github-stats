import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHCommitQueryBuilder;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.PagedIterable;
import org.kohsuke.github.PagedIterator;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GithubRestClientTest {

    private GithubRestClient restClient;
    @Mock
    private GitHub gitHub;
    @Mock
    private KafkaProducer<String, GithubFetcher.Commit> producer;

    @Before
    public void setup() throws IOException {
        restClient = new GithubRestClient(gitHub);

        // Mock commits
        GHCommit commit1 = Mockito.mock(GHCommit.class);
        GHUser user1 = Mockito.mock(GHUser.class);
        when(commit1.getAuthor()).thenReturn(user1);
        when(user1.getLogin()).thenReturn("login1");

        GHCommit commit2 = Mockito.mock(GHCommit.class);
        GHUser user2 = Mockito.mock(GHUser.class);
        when(commit2.getAuthor()).thenReturn(user2);
        when(user2.getLogin()).thenReturn("login2");

        // Mock Page iterator
        PagedIterator<GHCommit> pagedIterator = Mockito.mock(PagedIterator.class);

        Mockito.when(pagedIterator.hasNext())
                .thenReturn(true, false);
        Mockito.when(pagedIterator.nextPage())
                .thenReturn(Arrays.asList(commit1, commit2));

        // Mock getting commits
        GHRepository repository = Mockito.mock(GHRepository.class);
        GHCommitQueryBuilder queryBuilder = Mockito.mock(GHCommitQueryBuilder.class);
        PagedIterable<GHCommit> pagedIterable = Mockito.mock(PagedIterable.class);

        when(gitHub.getRepository("apache/cassandra")).thenReturn(repository);
        when(repository.queryCommits()).thenReturn(queryBuilder);
        when(queryBuilder.since(any())).thenReturn(queryBuilder);
        when(queryBuilder.list()).thenReturn(pagedIterable);
        when(pagedIterable.withPageSize(anyInt())).thenReturn(pagedIterable);
        when(pagedIterable.iterator()).thenReturn(pagedIterator);
    }

    @Test
    public void testProcessCommitsInBatches() {
        GithubFetcher.FetchRequest request = new GithubFetcher.FetchRequest();
        UUID fetchRequestId = UUID.randomUUID();
        request.setId(fetchRequestId);
        request.setOwner("apache");
        request.setRepo("cassandra");
        request.setSince(LocalDate.of(2024, Month.JUNE, 1));

        restClient.processCommitsInBatches(request, 2, (commits ->
                commits.forEach(commit -> {
                    String key = commit.getRepo() + ":" + commit.getFetchRequestId();
                    producer.send(new ProducerRecord<>("OUTPUT_TOPIC", key, commit));
                })));
        InOrder inOrder = inOrder(producer);
        inOrder.verify(producer)
                .send(new ProducerRecord<>("OUTPUT_TOPIC",
                        "apache:cassandra:" + fetchRequestId,
                        new GithubFetcher.Commit("apache:cassandra", "login1", fetchRequestId)));
        inOrder.verify(producer)
                .send(new ProducerRecord<>("OUTPUT_TOPIC",
                        "apache:cassandra:" + fetchRequestId,
                        new GithubFetcher.Commit("apache:cassandra", "login2", fetchRequestId)));
    }
}