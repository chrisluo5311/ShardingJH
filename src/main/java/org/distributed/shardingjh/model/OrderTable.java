package org.distributed.shardingjh.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "order_table")
public class OrderTable {

    @Id
    @Column(name = "order_id")
    private String orderId;

    @Column(name = "create_time")
    private LocalDateTime createTime;

    @Column(name = "is_paid")
    private Integer isPaid;

    @Column(name = "member_id")
    private String memberId;
}