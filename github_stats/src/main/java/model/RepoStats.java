package model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class RepoStats {

    private String repo;
    private TopFiveContributors topFiveContributors = new TopFiveContributors();
    private long totalCommits;
    private long totalCommitters;
}
