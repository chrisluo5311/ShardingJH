package org.distributed.shardingjh.controller.productcontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.model.Product;
import org.distributed.shardingjh.rabbitmq.ProductSyncProducer;
import org.distributed.shardingjh.service.Impl.ProductServiceImpl;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/product")
public class ProductController {

    @Resource
    ProductServiceImpl productService;

    @Resource
    ProductSyncProducer productSyncProducer;

    @RequestMapping(value = "/add", method = RequestMethod.POST)
    public MgrResponseDto<Product> addProduct(@RequestBody ProductRequest productRequest) {
        Product product = productService.addOrUpdateProduct(productRequest.getName(), productRequest.getPrice());
        productSyncProducer.publishProductUpdate("add", product.getId(), product.getName(), product.getPrice());
        return MgrResponseDto.success(product);
    }

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    public MgrResponseDto<Product> updateProduct(@RequestBody ProductRequest productRequest) {
        if (productService.getProduct(productRequest.getName()) == null) {
            return MgrResponseDto.error(MgrResponseCode.PRODUCT_NOT_FOUND);
        }
        Product product = productService.addOrUpdateProduct(productRequest.getName(), productRequest.getPrice());
        productSyncProducer.publishProductUpdate("update", product.getId(), product.getName(), product.getPrice());
        return MgrResponseDto.success(product);
    }

    @GetMapping("/delete")
    public MgrResponseDto<String> deleteProduct(@RequestParam String id) {
        if (!productService.deleteProduct(id)) {
            return MgrResponseDto.error(MgrResponseCode.PRODUCT_NOT_FOUND);
        }
        productSyncProducer.publishProductUpdate("delete", id, null, null);
        return MgrResponseDto.success("Product deleted successfully");
    }

    @GetMapping("/all")
    public List<Product> getAll() {
        return productService.getAllProducts();
    }
}
