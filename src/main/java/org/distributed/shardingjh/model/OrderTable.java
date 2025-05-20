package org.distributed.shardingjh.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "order_table")
public class OrderTable {

    @EmbeddedId
    @AttributeOverrides({
            @AttributeOverride(name = "orderId", column = @Column(name = "order_id")),
            @AttributeOverride(name = "version", column = @Column(name = "version"))
    })

    private OrderKey id;

    @Column(name = "create_time")
    private LocalDateTime createTime;

    @Column(name = "is_paid")
    private Integer isPaid;

    @Column(name = "member_id")
    private String memberId;

    // MVCC
    @Column(name = "expired_at")
    private LocalDateTime expiredAt;

    @Column(name = "is_deleted")
    private Integer isDeleted = 0;
}