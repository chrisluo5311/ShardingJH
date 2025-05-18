package org.distributed.shardingjh.service.Impl;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.RedisConst;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.OrderRepository;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.OrderService;
import org.distributed.shardingjh.sharding.Impl.RangeStrategy;
import org.springframework.data.redis.core.RedisTemplate;
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

    @Resource
    private RedisTemplate<String, OrderTable> redisTemplate;

    @Override
    public OrderTable saveOrder(RequestOrder requestOrder) {
        try {
            // Generate the order ID
            log.info("Save new order: {}", requestOrder);
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
            String key = RedisConst.REDIS_KEY_ORDER_PREFIX + orderId;
            redisTemplate.opsForValue().set(key, orderTable);
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
        try {
            log.info("Find Order between: {}, {}", startDate, endDate);
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
            } else {
                log.info("Order Cross Year Search ...");
                int startYear = startTime.getYear();
                int endYear = endTime.getYear();
                // if the start and end shard keys are different, then search in different shards
                for (int i = startYear; i <= endYear; i++) {
                    if (startTime.getYear() == i) {
                        ShardContext.setCurrentShard(startShardKey);
                        log.info("Searching shard key: {}", ShardContext.getCurrentShard());
                        List<OrderTable> startOrder = orderRepository.findAllByCreateTimeAfter(startTime);
                        result.addAll(startOrder);
                    } else if (endTime.getYear() == i) {
                        ShardContext.setCurrentShard(endShardKey);
                        log.info("Searching shard key: {}", ShardContext.getCurrentShard());
                        List<OrderTable> endOrder = orderRepository.findAllByCreateTimeBefore(endTime);
                        result.addAll(endOrder);
                    } else {
                        LocalDateTime middleStartTime = LocalDateTime.of(i, 1, 1, 0, 0);
                        String currShardKey = rangeStrategy.resolveShard(middleStartTime);
                        ShardContext.setCurrentShard(currShardKey);
                        log.info("Searching shard key: {}", ShardContext.getCurrentShard());
                        List<OrderTable> oldOrder = orderRepository.findAllByCreateTimeAfter(middleStartTime);
                        result.addAll(oldOrder);
                    }
                    ShardContext.clear();
                }
            }
            return result;
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }

    @Override
    public OrderTable findByIdAndCreateTime(String orderId, String createTime) {
        try {
            log.info("Find Order by ID: {}, createTime: {}", orderId, createTime);
            // Get the shard key based on the order creation time
            LocalDateTime startTime = LocalDate.parse(createTime).atStartOfDay();
            String shardKey = rangeStrategy.resolveShard(startTime);
            log.info("Order ID: {} routing to {}", orderId, shardKey);
            // Set the shard key in the context
            ShardContext.setCurrentShard(shardKey);
            // Find the order by ID
            OrderTable order = orderRepository.findById(orderId).orElse(null);
            return order;
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }

    @Override
    public OrderTable updateOrder(OrderTable newOrder) {
        try {
            log.info("Update Order: {}", newOrder.getOrderId());
            String shardKey = rangeStrategy.resolveShard(newOrder.getCreateTime());
            log.info("Order ID: {} routing to {}", newOrder.getOrderId(), shardKey);
            // Set the shard key in the context
            ShardContext.setCurrentShard(shardKey);
            String key = RedisConst.REDIS_KEY_ORDER_PREFIX + newOrder.getOrderId();
            redisTemplate.opsForValue().set(key, newOrder);
            orderRepository.save(newOrder);
            return newOrder;
        } finally {
            ShardContext.clear();
        }
    }

    @Override
    public void deleteOrder(OrderTable orderTable) {
        try {
            log.info("Delete Order: {}", orderTable.getOrderId());
            // find shard
            String shardKey = rangeStrategy.resolveShard(orderTable.getCreateTime());
            log.info("Order ID: {} routing to {}", orderTable.getOrderId(), shardKey);
            // Set the shard key in the context
            ShardContext.setCurrentShard(shardKey);
            orderRepository.deleteById(orderTable.getOrderId());
        } finally {
            ShardContext.clear();
        }

    }
}
