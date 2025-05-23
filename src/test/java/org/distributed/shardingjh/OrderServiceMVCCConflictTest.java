package org.distributed.shardingjh;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.model.OrderKey;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.OrderRepository;
import org.distributed.shardingjh.service.Impl.OrderServiceImpl;
import org.distributed.shardingjh.sharding.Impl.RangeStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class OrderServiceMVCCConflictTest {

    @Resource
    private OrderRepository orderRepository;

    @Resource
    private OrderServiceImpl orderService;

    @Resource
    private RangeStrategy rangeStrategy;

    @BeforeEach
    public void setup() {
        orderRepository.deleteAll();  // clean slate
    }

    @Test
    public void testUpdateOrder_withStaleVersion_shouldThrowConflict() {
        // Step 1: Insert version 1
        OrderTable original = new OrderTable();
        String orderId = "ORD123";
        LocalDateTime createTime = LocalDateTime.of(2025, 5, 18, 10, 0);

        original.setId(new OrderKey(orderId, 1));
        original.setCreateTime(createTime);
        original.setMemberId("user1");
        original.setIsPaid(0);
        original.setIsDeleted(0);
        original.setExpiredAt(null);

        String shardKey = rangeStrategy.resolveShard(createTime);
        ShardContext.setCurrentShard(shardKey);
        orderRepository.save(original);
        ShardContext.clear();

        // Step 2: Simulate a correct update (moves to version 2)
        OrderTable update1 = new OrderTable();
        update1.setId(new OrderKey(orderId, 1)); // correct expected version
        update1.setCreateTime(createTime);
        update1.setMemberId("user1");
        update1.setIsPaid(1); // change payment status
        update1.setIsDeleted(0);

        orderService.updateOrder(update1);

        // Step 3: Simulate a conflicting update using stale version 1
        OrderTable conflictingUpdate = new OrderTable();
        conflictingUpdate.setId(new OrderKey(orderId, 1)); // still thinks it's v1
        conflictingUpdate.setCreateTime(createTime);
        conflictingUpdate.setMemberId("user1");
        conflictingUpdate.setIsPaid(0); // another change
        conflictingUpdate.setIsDeleted(0);

        // Step 4: Expect exception
        Exception exception = assertThrows(IllegalStateException.class, () -> {
            orderService.updateOrder(conflictingUpdate);
        });

        assertTrue(exception.getMessage().contains("Version mismatch"));
    }
}
