package org.distributed.shardingjh.service.Impl;

import jakarta.annotation.Resource;
import org.distributed.shardingjh.model.Product;
import org.distributed.shardingjh.repository.product.ProductRepository;
import org.distributed.shardingjh.service.ProductService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductServiceImpl implements ProductService {

    @Resource
    ProductRepository productRepository;

    @Override
    public Product addOrUpdateProduct(String name, Integer price) {
        Product product = getProduct(name);
        if (product == null) {
            product = new Product();
            product.setId(java.util.UUID.randomUUID().toString());
        }
        product.setName(name);
        product.setPrice(price);
        return productRepository.save(product);
    }

    @Override
    public boolean deleteProduct(String id) {
        if (productRepository.existsById(id)) {
            productRepository.deleteById(id);
            return true;
        }
        return false;
    }

    @Override
    public Product getProduct(String name) {
        return productRepository.findByName(name);
    }

    @Override
    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }
}
