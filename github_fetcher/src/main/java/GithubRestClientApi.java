import java.util.List;
import java.util.function.Consumer;

public interface GithubRestClientApi {

    // This REST client is intended to process commits in batches for efficiency. It accepts Java Consumer lambda that
    // determines how each batch should be processed
    void processCommitsInBatches(GithubFetcher.FetchRequest request, int batchSize, Consumer<List<GithubFetcher.Commit>> processor);

}
