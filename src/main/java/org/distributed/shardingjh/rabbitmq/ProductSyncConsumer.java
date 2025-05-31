package org.distributed.shardingjh.rabbitmq;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.controller.productcontroller.ProductRequest;
import org.distributed.shardingjh.service.Impl.ProductServiceImpl;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Slf4j
@Service
public class ProductSyncConsumer {

    @Resource
    ProductServiceImpl productService;

    @Transactional
    @RabbitListener(queues = "${product.queue}", autoStartup = "false")
    public void receiveSync(Map<String, Object> msg) {
        log.info("Received product sync message: {}", msg);
        String op = (String) msg.get("op");
        String id = (String) msg.get("id");
        String name = (String) msg.get("name");
        Integer price = (Integer) msg.get("price");

        if ("add".equals(op) || "update".equals(op)) {
            ProductRequest productRequest = new ProductRequest();
            productRequest.setId(id);
            productRequest.setName(name);
            productRequest.setPrice(price);
            productService.addOrUpdateProduct(productRequest);
            log.info("[ProductSyncConsumer] Product added/updated (DB): {} - {}", id, name);
        } else if ("delete".equals(op)) {
            productService.deleteProduct(id);
            log.info("[ProductSyncConsumer] Product deleted (DB): {}", id);
        }
    }
}
