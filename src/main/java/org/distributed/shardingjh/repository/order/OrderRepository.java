package org.distributed.shardingjh.repository.order;

import io.lettuce.core.dynamic.annotation.Param;
import org.distributed.shardingjh.model.OrderTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<OrderTable, String>  {

    List<OrderTable> findByCreateTimeBetween(LocalDateTime createTimeAfter, LocalDateTime createTimeBefore);

    List<OrderTable> findAllByCreateTimeAfter(LocalDateTime createTimeAfter);

    List<OrderTable> findAllByCreateTimeBefore(LocalDateTime createTimeBefore);

}
