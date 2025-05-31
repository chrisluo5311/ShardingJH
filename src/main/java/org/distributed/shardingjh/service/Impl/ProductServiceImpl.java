package org.distributed.shardingjh.service.Impl;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.controller.productcontroller.ProductRequest;
import org.distributed.shardingjh.model.Product;
import org.distributed.shardingjh.repository.product.ProductRepository;
import org.distributed.shardingjh.service.ProductService;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ProductServiceImpl implements ProductService {

    @Resource
    ProductRepository productRepository;

    @Override
    public Product addOrUpdateProduct(ProductRequest productRequest) {
        Product product = getProduct(productRequest.getId());
        if (product == null) {
            log.info("Creating new product with ID: {}", productRequest.getId());
            product = new Product();
            product.setId(productRequest.getId());
        }
        product.setName(productRequest.getName());
        product.setPrice(productRequest.getPrice());
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
    public Product getProduct(String id) {
        return productRepository.findById(id).orElse(null);
    }

    @Override
    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }
}
