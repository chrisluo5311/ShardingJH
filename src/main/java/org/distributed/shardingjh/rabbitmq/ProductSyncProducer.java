package org.distributed.shardingjh.rabbitmq;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class ProductSyncProducer {

    @Resource
    RabbitTemplate rabbitTemplate;

    @Value("${product.exchange}")
    private String productExchange;

    /**
     * Publishes a product update message to the RabbitMQ exchange.
     * Fanout exchanges ignore the routing key completely, so routingKey is empty
     * Fanout broadcast every message to all queues bound to them
     * */
    public void publishProductUpdate(String op, String id, String name, Integer price) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("op", op);
        msg.put("id", id);
        msg.put("name", name);
        msg.put("price", price);
        rabbitTemplate.convertAndSend(productExchange, "", msg);
        log.info("[ProductSyncProducer] Published product sync message: {}", msg);
    }
}
