package model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class Contributor {

    public Contributor() {
    }

    public Contributor(String repo, String authorName) {
        this.repo = repo;
        this.email = authorName;
    }

    String repo;
    String email;
}
