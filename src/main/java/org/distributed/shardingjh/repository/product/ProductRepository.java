package org.distributed.shardingjh.repository.product;

import org.distributed.shardingjh.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends JpaRepository<Product, String> {

    Product findByName(String name);
}
