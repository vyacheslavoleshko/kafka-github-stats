package model;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TopFiveContributorsTest {

    TopFiveContributors topFive;

    @Before
    public void setup() {
        topFive = new TopFiveContributors();
    }

    @Test
    public void testIterator() {
        var c1 = new ContributorWithCount("repo2", "email2.gmail.com", 2L);
        var c2 = new ContributorWithCount("repo1", "email.gmail.com", 3L);
        var c3 = new ContributorWithCount("repo3", "email.gmail.com", 1L);
        var c4 = new ContributorWithCount("repo3", "email.gmail.com", 5L);
        var c5 = new ContributorWithCount("repo3", "email.gmail.com", 0L);
        var c6 = new ContributorWithCount("repo3", "email.gmail.com", 1L);
        var c7 = new ContributorWithCount("repo3", "email.gmail.com", 11L);
        var c8 = new ContributorWithCount("repo3", "email.gmail.com", -1L);
        var contributors = Arrays.asList(
            c1, c2, c3, c4, c5, c6, c7, c8
        );
        for (var c : contributors) {
            topFive.add(c);
        }
        ContributorWithCount[] expected = new ContributorWithCount[]{c7, c4, c2, c1, c3};
        int i = 0;
        while (topFive.iterator().hasNext()) {
            assertEquals(expected[i], topFive.iterator().next());
            i++;
        }
        assertEquals(5, i);
    }
}