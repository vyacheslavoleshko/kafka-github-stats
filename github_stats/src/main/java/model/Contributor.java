package model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@EqualsAndHashCode
public class Contributor {

    public Contributor() {
    }

    public Contributor(String repo, String login, UUID fetchRequestId) {
        this.repo = repo;
        this.login = login;
        this.fetchRequestId = fetchRequestId;
    }

    String repo;
    String login;
    UUID fetchRequestId;

    @JsonIgnore
    public Key getKey() {
        return new Key(repo, fetchRequestId);
    }
}
