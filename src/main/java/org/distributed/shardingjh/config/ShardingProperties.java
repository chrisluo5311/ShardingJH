package org.distributed.shardingjh.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "sharding")
public class ShardingProperties {

    // Sharding strategy type
    private Map<String, String> lookup;
}
