package org.distributed.shardingjh.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;

import java.util.Date;

@Entity
@Data
@Table(name = "order_table")
public class OrderTable {

    @Id
    @Column(name = "order_id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer orderId;

    @Column(name = "is_paid")
    private Integer isPaid;

    @CreationTimestamp
    @JsonIgnore
    @Column(name = "create_time")
    private Date createTime;
}
