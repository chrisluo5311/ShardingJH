package org.distributed.shardingjh.rabbitmq;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class ProductSyncProducer {

    @Resource
    RabbitTemplate rabbitTemplate;

    @Value("${product.exchange}")
    private String productExchange;

    // For simple demo: use an in-memory Set (production: use a DB table or persistent queue)
    private final Set<Map<String, Object>> dirtyMessages = ConcurrentHashMap.newKeySet();

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
        try {
            rabbitTemplate.convertAndSend(productExchange, "", msg);
            log.info("[ProductSyncProducer] Published product sync message: {}", msg);
        } catch (Exception e) {
            log.error("[ProductSyncProducer] Failed to publish product sync message, marking as dirty: {} - Exception: {}", msg, e.getMessage());
            dirtyMessages.add(msg);
        }
    }

    // Scheduled background job to retry "dirty" messages every minute
    @Scheduled(fixedDelay = 60000)
    public void retryDirtyMessages() {
        for (Map<String, Object> msg : new HashSet<>(dirtyMessages)) {
            try {
                rabbitTemplate.convertAndSend(productExchange, "", msg);
                log.info("[ProductSyncProducer] Retried and published dirty message: {}", msg);
                dirtyMessages.remove(msg);
            } catch (Exception e) {
                log.warn("[ProductSyncProducer] Retry failed for dirty message: {} - Exception: {}", msg, e.getMessage());
            }
        }
    }
}
