package model;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Class to keep Top-5 contributors based on their commits count.
 */
public class TopFiveContributors implements Iterable<ContributorWithCount> {

    private final Comparator<ContributorWithCount> ASC_COMPARATOR =
            Comparator.comparing(ContributorWithCount::getCommitCount)
                    .thenComparing(ContributorWithCount::getEmail);
    private final Comparator<ContributorWithCount> DESC_COMPARATOR = ASC_COMPARATOR.reversed();

    private final PriorityQueue<ContributorWithCount> topFive = new PriorityQueue<>(DESC_COMPARATOR);
    private final PriorityQueue<ContributorWithCount> bottomFive = new PriorityQueue<>(ASC_COMPARATOR);

    private final int maxSize = 5;
    private final ObjectMapper mapper = new ObjectMapper();

    public TopFiveContributors() {

    }

    public void add(ContributorWithCount contributor) {
        topFive.add(contributor);
        bottomFive.add(contributor);
        if (topFive.size() > maxSize) {
            var smallestCommitsContributor = bottomFive.poll();
            topFive.remove(smallestCommitsContributor);
        }
    }

    @SneakyThrows
    @Override
    public String toString() {
        List<ContributorWithCount> top = new ArrayList<>();
        for (ContributorWithCount c : this) {
            top.add(c);
        }
        return mapper.writeValueAsString(top);
    }

    public void remove(ContributorWithCount contributor) {
        topFive.remove(contributor);
        bottomFive.remove(contributor);
    }


    @Override
    public Iterator<ContributorWithCount> iterator() {
        return new Iterator<>() {
            final List<ContributorWithCount> holder = new ArrayList<>();

            @Override
            public boolean hasNext() {
                boolean hasNext = topFive.peek() != null;
                if (!hasNext) {
                    topFive.addAll(holder);
                }
                return hasNext;
            }

            @Override
            public ContributorWithCount next() {
                var contributor = topFive.poll();
                if (contributor != null) holder.add(contributor);
                return contributor;
            }
        };
    }

}
