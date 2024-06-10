package model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class ContributorWithCount {

    public ContributorWithCount(String repo, String email, Long commitCount) {
        this.repo = repo;
        this.email = email;
        this.commitCount = commitCount;
    }

    public ContributorWithCount() {
    }

    String repo;
    String email;
    Long commitCount;
}
