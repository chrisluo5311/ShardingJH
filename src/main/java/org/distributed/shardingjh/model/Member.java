package org.distributed.shardingjh.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

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
}