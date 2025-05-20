package org.distributed.shardingjh.service.Impl;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.weaver.ast.Or;
import org.distributed.shardingjh.common.constant.RedisConst;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.model.OrderKey;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.OrderRepository;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.OrderService;
import org.distributed.shardingjh.sharding.Impl.RangeStrategy;
import org.springframework.beans.BeanUtils;
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

    private void logRouting(String orderId, String shardKey) {
        log.info("Order ID: {} routing to {}", orderId, shardKey);
    }

    @Override
    public OrderTable saveOrder(RequestOrder requestOrder) {
        try {
            // Generate the order ID
            log.info("Save new order: {}", requestOrder);
            String orderId = OrderIdGenerator.generateOrderId(requestOrder.getCreateTime(), requestOrder.getMemberId());
            log.info("New order ID: {}", orderId);
            OrderTable orderTable = new OrderTable();
            orderTable.setMemberId(requestOrder.getMemberId());
            orderTable.setCreateTime(requestOrder.getCreateTime());
            orderTable.setIsPaid(requestOrder.getIsPaid());
            // Get the shard key based on the order creation time and set the shard key
            String shardKey = rangeStrategy.resolveShard(orderTable.getCreateTime());
            logRouting(orderId, shardKey);
//            String redisKey = RedisConst.REDIS_KEY_ORDER_PREFIX + orderId;
//            redisTemplate.opsForValue().set(redisKey, orderTable);
            ShardContext.setCurrentShard(shardKey);
            // Expire previous version
            OrderTable current = orderRepository.findCurrentByOrderId(orderId).orElse(null);
            if (current != null) {
                current.setExpiredAt(LocalDateTime.now());
                // persist expired version
                orderRepository.save(current);
                orderTable.setId(new OrderKey(orderId, current.getId().getVersion() + 1));
            } else {
                orderTable.setId(new OrderKey(orderId, 1));
            }

            // Set the current version and not deleted
            orderTable.setExpiredAt(null);
            orderTable.setIsDeleted(0);
            orderRepository.save(orderTable);
            return orderTable;
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }

    /**
     * Soft delete the order
     * 1. expiring the latest version
     * 2. set the isDeleted flag to true
     * */
    @Override
    public void deleteOrder(OrderTable orderTable) {
        try {
            log.info("Delete Order: {}", orderTable.getId().getOrderId());
            // Find shard and Set the shard key
            String shardKey = rangeStrategy.resolveShard(orderTable.getCreateTime());
            logRouting(orderTable.getId().getOrderId(), shardKey);
            ShardContext.setCurrentShard(shardKey);
            // Expire previous version
            OrderTable current = orderRepository.findCurrentByOrderId(orderTable.getId().getOrderId()).orElse(null);
            if (current != null) {
                current.setExpiredAt(LocalDateTime.now());
                orderRepository.save(current);

                // Insert deleted record (for audit/history)
                OrderTable deleted = new OrderTable();
                BeanUtils.copyProperties(current, deleted);
                deleted.setId(new OrderKey(orderTable.getId().getOrderId(), current.getId().getVersion()+1));
                deleted.setExpiredAt(null);
                deleted.setIsDeleted(1);
                orderRepository.save(deleted);
            }
        } finally {
            ShardContext.clear();
        }
    }

    /**
     *
     * Replace old versions and insert new ones
     *
     * */
    @Override
    public OrderTable updateOrder(OrderTable newOrder) {
        try {
            log.info("Update Order: {}", newOrder.getId().getOrderId());
            // Find shard and Set the shard key
            String shardKey = rangeStrategy.resolveShard(newOrder.getCreateTime());
            logRouting(newOrder.getId().getOrderId(), shardKey);
            ShardContext.setCurrentShard(shardKey);
            // Expire previous version
            OrderTable current = orderRepository.findCurrentByOrderId(newOrder.getId().getOrderId()).orElse(null);
            if (current != null) {
                log.info("Current Order version: {}", current.getId().getVersion());
                current.setExpiredAt(LocalDateTime.now());
                orderRepository.save(current);  // persist expired version
                newOrder.setId(new OrderKey(newOrder.getId().getOrderId(), current.getId().getVersion()+1));
            } else {
                newOrder.getId().setVersion(1);
            }

            // current version, not deleted
            newOrder.setExpiredAt(null);
            newOrder.setIsDeleted(0);
            String redisKey = RedisConst.REDIS_KEY_ORDER_PREFIX + newOrder.getId().getOrderId();
//            redisTemplate.opsForValue().set(redisKey, newOrder);
            orderRepository.save(newOrder);
            return newOrder;
        } finally {
            ShardContext.clear();
        }
    }

    @Override
    public List<OrderTable> findByCreateTimeBetween(String startDate, String endDate) {
        try {
            log.info("Find Order between: {}, {}", startDate, endDate);
            List<OrderTable> result = new ArrayList<>();
            // Transform the String date "2024-04-25" to a LocalDateTime object
            LocalDateTime startTime = LocalDate.parse(startDate).atStartOfDay();
            LocalDateTime endTime = LocalDate.parse(endDate).atTime(LocalTime.MAX);
            String startShardKey = rangeStrategy.resolveShard(startTime);
            String endShardKey = rangeStrategy.resolveShard(endTime);
            log.info("Start ShardKey: {}, End ShardKey: {}", startShardKey, endShardKey);
            // if the start and end shard keys are the same, then search in the same shard
            if (startShardKey.equals(endShardKey)) {
                ShardContext.setCurrentShard(startShardKey);
                log.info("Current shard key: {}", ShardContext.getCurrentShard());
                return orderRepository.findValidOrdersBetween(startTime, endTime);
            } else {
                // if the start and end shard keys are different, then search in different shards
                log.info("Find Order Across Multiple Year ...");
                int startYear = startTime.getYear();
                int endYear = endTime.getYear();
                for (int i = startYear; i <= endYear; i++) {
                    if (startTime.getYear() == i) {
                        ShardContext.setCurrentShard(startShardKey);
                        log.info("Searching shard key: {}", ShardContext.getCurrentShard());
                        List<OrderTable> startOrder = orderRepository.findValidOrdersAfter(startTime);
                        result.addAll(startOrder);
                    } else if (endTime.getYear() == i) {
                        ShardContext.setCurrentShard(endShardKey);
                        log.info("Searching shard key: {}", ShardContext.getCurrentShard());
                        List<OrderTable> endOrder = orderRepository.findValidOrdersBefore(endTime);
                        result.addAll(endOrder);
                    } else {
                        LocalDateTime middleStartTime = LocalDateTime.of(i, 1, 1, 0, 0);
                        String currShardKey = rangeStrategy.resolveShard(middleStartTime);
                        ShardContext.setCurrentShard(currShardKey);
                        log.info("Searching shard key: {}", ShardContext.getCurrentShard());
                        List<OrderTable> oldOrder = orderRepository.findValidOrdersAfter(middleStartTime);
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
            logRouting(orderId, shardKey);
            ShardContext.setCurrentShard(shardKey);
            // Find the order by ID
            return orderRepository.findCurrentByOrderId(orderId).orElse(null);
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }

    @Override
    public List<OrderTable> findAllVersions(String orderId, String createTime) {
        try {
            log.info("Fetching all versions of order: {}", orderId);
            // Get the shard key based on the order creation time
            LocalDateTime startTime = LocalDate.parse(createTime).atStartOfDay();
            String shardKey = rangeStrategy.resolveShard(startTime);
            logRouting(orderId, shardKey);
            ShardContext.setCurrentShard(shardKey);

            // Assume latest createTime (or any createTime) is available for routing
            OrderTable current = orderRepository.findCurrentByOrderId(orderId).orElse(null);
            return (current == null) ? List.of() : orderRepository.findAllVersionsByOrderId(orderId);
        } finally {
            ShardContext.clear();
        }
    }
}
