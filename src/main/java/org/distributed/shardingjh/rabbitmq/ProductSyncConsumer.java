package org.distributed.shardingjh.rabbitmq;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.service.Impl.ProductServiceImpl;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Slf4j
@Service
public class ProductSyncConsumer {

    @Resource
    ProductServiceImpl productService;

    @Transactional
    @RabbitListener(queues = "${product.queue}")
    public void receiveSync(Map<String, Object> msg) {
        log.info("Received product sync message: {}", msg);
        String op = (String) msg.get("op");
        Integer id = (Integer) msg.get("id");
        String name = (String) msg.get("name");
        Integer price = (Integer) msg.get("price");

        if ("add".equals(op) || "update".equals(op)) {
            productService.addOrUpdateProduct(name, price);
            log.info("Product added/updated (DB): {} - {}", id, name);
        } else if ("delete".equals(op)) {
            productService.deleteProduct(id);
            log.info("Product deleted (DB): {}", id);
        }
    }
}
