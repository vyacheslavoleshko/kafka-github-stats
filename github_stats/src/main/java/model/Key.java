package model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@Data
@EqualsAndHashCode
public class Key {
    private String repo;
    private UUID requestId;

    public Key(String repo, UUID requestId) {
        this.repo = repo;
        this.requestId = requestId;
    }
}
