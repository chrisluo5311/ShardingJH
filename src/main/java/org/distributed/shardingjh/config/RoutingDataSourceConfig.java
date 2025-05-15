package org.distributed.shardingjh.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Setting up a routing data source for sharding.
 * This class defines a bean that creates a data source capable of routing queries
 * to different underlying shards depending on the routing logic.
 * @author chris
 */
@Configuration
public class RoutingDataSourceConfig {

    @Bean(name = "shardRoutingDataSource")
    @Primary
    public DataSource dataSource(
            @Qualifier("shardCommon1DataSource") DataSource shardCommon1,
            @Qualifier("shardCommon2DataSource") DataSource shardCommon2) {
        Map<Object, Object> targetDataSources = new HashMap<>();
        targetDataSources.put("shard_common_1", shardCommon1);
        targetDataSources.put("shard_common_2", shardCommon2);

        ShardRoutingDataSource routingDataSource = new ShardRoutingDataSource();
        routingDataSource.setTargetDataSources(targetDataSources);
        // set the default shard to shard1
        routingDataSource.setDefaultTargetDataSource(shardCommon1);
        return routingDataSource;
    }

    @Bean(name = "shardOrderRoutingDataSource")
    public DataSource dataSource2(
            @Qualifier("shardOrder2024DataSource") DataSource shardOrder2024,
            @Qualifier("shardOrder2025DataSource")DataSource shardOrder2025,
            @Qualifier("shardOrderOldDataSource") DataSource shardOrderOld) {
        Map<Object, Object> targetDataSources = new HashMap<>();
        targetDataSources.put("shard_order_2024", shardOrder2024);
        targetDataSources.put("shard_order_2025", shardOrder2025);
        targetDataSources.put("shard_order_old", shardOrderOld);
        ShardRoutingDataSource routingDataSource = new ShardRoutingDataSource();
        routingDataSource.setTargetDataSources(targetDataSources);
        // set the default shard to shard1
        routingDataSource.setDefaultTargetDataSource(shardOrder2025);
        return routingDataSource;
    }


}
