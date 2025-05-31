package org.distributed.shardingjh.service;

import org.distributed.shardingjh.model.Product;

import java.util.List;

public interface ProductService {

    Product addOrUpdateProduct(String name, Integer price);

    boolean deleteProduct(String id);

    Product getProduct(String name);

    List<Product> getAllProducts();
}
