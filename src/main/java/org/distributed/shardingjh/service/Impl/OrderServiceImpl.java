package org.distributed.shardingjh.service.Impl;

import jakarta.annotation.Resource;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.RollbackException;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.model.OrderKey;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.OrderRepository;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.OrderService;
import org.distributed.shardingjh.sharding.Impl.RangeStrategy;
import org.springframework.beans.BeanUtils;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.sqlite.SQLiteException;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // For testing MVCC concurrency conflict
    private static final AtomicBoolean firstFailureSimulated = new AtomicBoolean(false);

    // EntityManager is used to interact with the persistence context (i.e., to perform database operations like queries, inserts, updates, and deletes).
    // Each EntityManager instance represents a single unit of work and should be closed after use to release resources.
    @PersistenceContext(unitName = "shardingOrder")
    private EntityManager em;

    @Resource
    private PlatformTransactionManager txManager;

    private void logRouting(String orderId, String shardKey) {
        log.info("[Service] Order ID: {} routing to {}", orderId, shardKey);
    }

    @Override
    public OrderTable saveOrder(RequestOrder requestOrder) {
        try {
            // Generate the order ID
            log.info("Save new order: {}", requestOrder);
            String orderId = requestOrder.getOrderId();
            log.info("New order ID: {}", orderId);
            OrderTable orderTable = new OrderTable();
            orderTable.setMemberId(requestOrder.getMemberId());
            orderTable.setCreateTime(requestOrder.getCreateTime());
            orderTable.setIsPaid(requestOrder.getIsPaid());
            orderTable.setPrice(requestOrder.getPrice());

            // Get the shard key based on the order creation time and set the shard key
            String shardKey = rangeStrategy.resolveShard(orderTable.getCreateTime());
            logRouting(orderId, shardKey);
            ShardContext.setCurrentShard(shardKey);

            // Expire previous version
            OrderTable current = orderRepository.findCurrentByOrderId(orderId).orElse(null);
            if (current != null) {
                current.setExpiredAt(LocalDateTime.now());
                // persist expired version
                orderRepository.save(current);
                log.info("Older version of Order: {} expired successfully", orderId);
                orderTable.setId(new OrderKey(orderId, current.getId().getVersion() + 1));
            } else {
                orderTable.setId(new OrderKey(orderId, 1));
            }

            // Set the current version and not deleted
            orderTable.setExpiredAt(null);
            orderTable.setIsDeleted(0);
            orderRepository.save(orderTable);
            log.info("Order {} version:{} saved successfully", orderId, orderTable.getId().getVersion());
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
                log.info("Older version of Order: {} expired successfully", orderTable.getId().getOrderId());

                // Insert deleted record (for audit/history)
                OrderTable deleted = new OrderTable();
                BeanUtils.copyProperties(current, deleted);
                deleted.setId(new OrderKey(orderTable.getId().getOrderId(), current.getId().getVersion()+1));
                deleted.setExpiredAt(null);
                deleted.setIsDeleted(1);
                orderRepository.save(deleted);
                log.info("Order {} deleted successfully", orderTable.getId().getOrderId());
            }
        } finally {
            ShardContext.clear();
        }
    }

    /**
     * Replace old versions and insert new ones
     * */
    @Override
    public OrderTable updateOrder(OrderTable toUpdateOrder) {
        return updateWithRetryAndRollback(toUpdateOrder);
    }

    public OrderTable updateWithRetryAndRollback(OrderTable toUpdateOrder) {
        String orderId = toUpdateOrder.getId().getOrderId();
        log.info("[Manual TX] Start transactional update for: {}", orderId);

        // Reason for multiple attempts:
        // To generate MVCC DB_CONFLICT error because the first attempt will fail due to sqlite "DB locked"
        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            // Shard routing
            String shardKey = rangeStrategy.resolveShard(toUpdateOrder.getCreateTime());
            log.info("Routing order {} to shard {}", orderId, shardKey);
            ShardContext.setCurrentShard(shardKey);
            EntityManager newEm = em.getEntityManagerFactory().createEntityManager();
            try {
                newEm.getTransaction().begin();

                // MVCC check - Fetch current version
                OrderTable current = orderRepository.findCurrentByOrderId(orderId)
                        .orElseThrow(() -> new IllegalStateException("No existing order found"));

                int expectedVersion = toUpdateOrder.getId().getVersion();
                int actualVersion = current.getId().getVersion();

                log.info("[MVCC] Expected: {}, Actual: {}", expectedVersion, actualVersion);
                if (expectedVersion != actualVersion) {
                    throw new IllegalStateException("Version mismatch: expected " + expectedVersion + ", actual " + actualVersion);
                }

                // Expire current
                current.setExpiredAt(LocalDateTime.now());
                newEm.merge(current);

                // Insert new version
                int nextVersion = actualVersion + 1;
                OrderTable copiedOrder = new OrderTable();
                BeanUtils.copyProperties(toUpdateOrder, copiedOrder);
                copiedOrder.setId(new OrderKey(orderId, nextVersion));
                copiedOrder.setExpiredAt(null);
                copiedOrder.setIsDeleted(0);
                newEm.persist(copiedOrder);
                log.info("Older version expired, new version set to {}", nextVersion);

                 // TODO [Comment out if not test] Test rollback
//                if (attempt == 1) {
//                    throw new RuntimeException("Simulated failure");
//                }

                newEm.getTransaction().commit();
                log.info("Transaction committed on attempt {}", attempt);
                return toUpdateOrder;
            } catch (CannotAcquireLockException | RollbackException e) {
                log.error("DB locked. Attempt {}/{} failed. Retrying...", attempt, maxRetries);
                sleep(attempt * 100L);
                newEm.getTransaction().rollback();
            } catch (Exception e) {
                log.error("Rolling back transaction due to: {}", e.getMessage());
                newEm.getTransaction().rollback();
                if (e.getMessage().equals("Simulated failure")) {
                    throw new RuntimeException("Simulated failure");
                }
            } finally {
                newEm.close();
                ShardContext.clear();
            }
        }
        throw new IllegalStateException("All retry attempts failed for update");
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {}
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
