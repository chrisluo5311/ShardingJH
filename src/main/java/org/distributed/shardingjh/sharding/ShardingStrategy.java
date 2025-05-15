package org.distributed.shardingjh.sharding;

public interface ShardingStrategy {

    String resolveShard(Object key);
}
