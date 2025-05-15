package org.distributed.shardingjh.sharding.Impl;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.ShardConst;
import org.distributed.shardingjh.config.ShardingProperties;
import org.distributed.shardingjh.sharding.ShardingStrategy;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HashStrategy implements ShardingStrategy {

    @Resource
    private ShardingProperties shardingProperties;

    @Override
    public String resolveShard(Object id) {
        String userId = (String) id;
        if (userId == null) {
            log.error("User ID cannot be null");
            throw new IllegalArgumentException("User ID cannot be null");
        }
        log.info("Hash Code of user ID: {}", userId.hashCode());
        int shardIndex = Math.abs(userId.hashCode()) % ShardConst.TOTAL_SHARD_COMMON_COUNT + 1;
        return shardingProperties.getLookup().get(ShardConst.SHARD_COMMON_PREFIX + shardIndex);

    }
}
