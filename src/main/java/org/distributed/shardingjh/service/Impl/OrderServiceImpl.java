package org.distributed.shardingjh.service.Impl;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.OrderRepository;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.OrderService;
import org.distributed.shardingjh.sharding.Impl.RangeStrategy;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class OrderServiceImpl implements OrderService {

    @Resource
    private RangeStrategy rangeStrategy;

    @Resource
    private OrderRepository orderRepository;

    @Override
    public OrderTable saveOrder(RequestOrder requestOrder) {
        try {
            // Generate the order ID
            log.info("orderTable: {}", requestOrder);
            String orderId = OrderIdGenerator.generateOrderId(requestOrder.getCreateTime(), requestOrder.getMemberId());
            log.info("Order ID: {}", orderId);
            OrderTable orderTable = new OrderTable();
            orderTable.setOrderId(orderId);
            orderTable.setMemberId(requestOrder.getMemberId());
            orderTable.setCreateTime(requestOrder.getCreateTime());
            orderTable.setIsPaid(requestOrder.getIsPaid());
            // Get the shard key based on the order creation time
            String shardKey = rangeStrategy.resolveShard(orderTable.getCreateTime());
            log.info("Order ID: {} routing to {}", orderId, shardKey);
            // Set the shard key in the context
            ShardContext.setCurrentShard(shardKey);
            orderRepository.save(orderTable);
            return orderTable;
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }

    }

    @Override
    public List<OrderTable> findByCreateTimeBetween(String startDate, String endDate) {
        List<OrderTable> result = new ArrayList<>();
        // Transform the String date "2024-04-25" to a Date object
        LocalDateTime startTime = LocalDate.parse(startDate).atStartOfDay();
        LocalDateTime endTime = LocalDate.parse(endDate).atTime(LocalTime.MAX);
        String startShardKey = rangeStrategy.resolveShard(startTime);
        String endShardKey = rangeStrategy.resolveShard(endTime);
        log.info("Start ShardKey: {}, End ShardKey: {}", startShardKey, endShardKey);
        // if the start and end shard keys are the same, then search in the same shard
        if (startShardKey.equals(endShardKey)) {
            ShardContext.setCurrentShard(startShardKey);
            log.info("Current shard key: {}", ShardContext.getCurrentShard());
            return orderRepository.findByCreateTimeBetween(startTime, endTime);
//            return orderRepository.findAllByCreateTimeAfter(startTime);
        } else {
            // if the start and end shard keys are different, then search in different shards
            ShardContext.setCurrentShard(startShardKey);
            // Start date to the end of that year
            List<OrderTable> startOrder = orderRepository.findAllByCreateTimeAfter(startTime);
            result.addAll(startOrder);

            ShardContext.setCurrentShard(endShardKey);
            List<OrderTable> endOrder = orderRepository.findAllByCreateTimeBefore(endTime);
            result.addAll(endOrder);

            return result;
        }
    }
}
