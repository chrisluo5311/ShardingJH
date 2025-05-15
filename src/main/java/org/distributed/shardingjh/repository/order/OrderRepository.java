package org.distributed.shardingjh.repository.order;

import org.distributed.shardingjh.model.OrderTable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderTable, Integer>  {
}
