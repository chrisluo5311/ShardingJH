package org.distributed.shardingjh.repository.order;

import io.lettuce.core.dynamic.annotation.Param;
import org.distributed.shardingjh.model.OrderTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<OrderTable, String>  {

    @Query("SELECT o FROM OrderTable o WHERE o.id.orderId = :orderId AND o.expiredAt IS NULL AND o.isDeleted = 0")
    Optional<OrderTable> findCurrentByOrderId(@Param("orderId") String orderId);

    @Query("SELECT o FROM OrderTable o WHERE o.createTime BETWEEN :start AND :end AND o.expiredAt IS NULL AND o.isDeleted = 0")
    List<OrderTable> findValidOrdersBetween(@Param("start") LocalDateTime start, @Param("end") LocalDateTime end);

    @Query("SELECT o FROM OrderTable o " +
            "WHERE o.createTime >= :createTimeAfter " +
            "AND o.expiredAt IS NULL AND o.isDeleted = 0")
    List<OrderTable> findValidOrdersAfter(@Param("createTimeAfter") LocalDateTime createTimeAfter);

    @Query("SELECT o FROM OrderTable o " +
            "WHERE o.createTime <= :createTimeBefore " +
            "AND o.expiredAt IS NULL AND o.isDeleted = 0")
    List<OrderTable> findValidOrdersBefore(@Param("createTimeBefore") LocalDateTime createTimeBefore);

    @Query("SELECT o FROM OrderTable o WHERE o.id.orderId = :orderId ORDER BY o.id.version ASC")
    List<OrderTable> findAllVersionsByOrderId(@Param("orderId") String orderId);
}
