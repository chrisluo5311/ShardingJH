package org.distributed.shardingjh.sharding.Impl;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.ShardConst;
import org.distributed.shardingjh.config.ShardingProperties;
import org.distributed.shardingjh.sharding.ShardingStrategy;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Slf4j
@Component
public class RangeStrategy implements ShardingStrategy {

    @Resource
    private ShardingProperties shardingProperties;

    @Override
    public String resolveShard(Object key) {
        LocalDateTime date = (LocalDateTime) key;
        if (date == null) {
            log.error("Order Date cannot be null");
            throw new IllegalArgumentException("Invalid Order: Date cannot be null");
        }
        String year = String.valueOf(date.getYear());
        log.info("[Sharding] Order Date: {}", date.toString());
        if (date.isEqual(LocalDate.of(2025, 1, 1).atStartOfDay())
                || date.isAfter(LocalDate.of(2025, 1, 1).atStartOfDay())) {
            // e.g., key is "ORDER_2025"
            return shardingProperties.getLookup().get(ShardConst.SHARD_ORDER_PREFIX + year);
        } else if (date.isEqual(LocalDate.of(2024, 1, 1).atStartOfDay())
                || date.isAfter(LocalDate.of(2024, 1, 1).atStartOfDay())) {
            // e.g., key is "ORDER_2024"
            return shardingProperties.getLookup().get(ShardConst.SHARD_ORDER_PREFIX + year);
        } else {
            // Order before 2024 goes to ORDER_old (e.g., key is "ORDER_old")
            return shardingProperties.getLookup().get(ShardConst.SHARD_ORDER_PREFIX + ShardConst.SHARD_ORDER_OLD);
        }
    }
}
