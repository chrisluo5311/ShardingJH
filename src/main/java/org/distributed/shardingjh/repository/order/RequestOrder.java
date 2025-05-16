package org.distributed.shardingjh.repository.order;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class RequestOrder {

    private LocalDateTime createTime;

    private Integer isPaid;

    private String memberId;
}
