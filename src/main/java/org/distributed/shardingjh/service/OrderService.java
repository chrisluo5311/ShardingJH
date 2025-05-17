package org.distributed.shardingjh.service;

import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.RequestOrder;

import java.util.List;

public interface OrderService {

    OrderTable saveOrder(RequestOrder orderTable);

    List<OrderTable> findByCreateTimeBetween(String startDate, String endDate);

    OrderTable findByIdAndCreateTime(String orderId, String createTime);
}
