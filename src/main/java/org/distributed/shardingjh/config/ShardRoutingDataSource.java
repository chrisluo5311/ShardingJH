package org.distributed.shardingjh.config;

import org.distributed.shardingjh.context.ShardContext;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * Dynamically route db operations to the right shard based on shard key
 * @author chris
 * */
public class ShardRoutingDataSource extends AbstractRoutingDataSource {
    @Override
    protected Object determineCurrentLookupKey() {
        // e.g., "shard1"
        return ShardContext.getCurrentShard();
    }
}
