package org.distributed.shardingjh.config;

import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

@Configuration
public class RabbitConfig {

    @Value("${product.exchange}")
    private String productExchange;

    @Value("${product.queue}")
    private String productQueue;

    @Bean
    public Jackson2JsonMessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /**
     * Creates a fanout exchange named as configured in application.properties
     * Durable: true means the exchange will survive a broker restart
     * Auto-delete: false means the exchange will not be deleted when the last queue is unbound
     */
    @Bean
    public FanoutExchange productSyncExchange() {
        return new FanoutExchange(productExchange, true, false);
    }

    /**
     * Durable : true means the queue will survive a broker restart
     * Exclusive: false means the queue can be used by other connections
     * Auto-delete: false means the queue will not be deleted when the last consumer unsubscribes
     *
     * */
    @Bean
    public Queue productSyncQueue() {
        return new Queue(productQueue, true, false, false);
    }

    /**
     * Binds the queue to the exchange so this instance receives all messages published to the exchange
     * */
    @Bean
    public Binding binding(FanoutExchange productSyncExchange, Queue productSyncQueue) {
        return BindingBuilder.bind(productSyncQueue).to(productSyncExchange);
    }
}
