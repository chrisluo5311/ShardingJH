package org.distributed.shardingjh.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;
import java.util.UUID;

@Entity
@Data
@Table(name = "member")
public class Member {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private String id;

    @NotNull(message = "Name cannot be null")
    @Size(max = 20)
    @Column(name = "name")
    private String name;

    // Default constructor will generate the UUID during instance creation.
    public Member() {
        this.id = UUID.randomUUID().toString();
    }
}