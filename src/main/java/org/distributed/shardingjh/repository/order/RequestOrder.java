package org.distributed.shardingjh.repository.order;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.distributed.shardingjh.service.Impl.OrderIdGenerator;

import java.time.LocalDateTime;

@Getter
@Setter
public class RequestOrder {

    private String orderId;

    private LocalDateTime createTime;

    private Integer isPaid;

    private String memberId;

    private Integer price;

    @JsonIgnore
    public void generateOrderId() {
        this.orderId = OrderIdGenerator.generateOrderId(createTime, memberId);
    }
}
