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
            @Qualifier("shard1DataSource") DataSource shard1,
            @Qualifier("shard2DataSource") DataSource shard2) {

        Map<Object, Object> targetDataSources = new HashMap<>();
        targetDataSources.put("shard1", shard1);
        targetDataSources.put("shard2", shard2);

        ShardRoutingDataSource routingDataSource = new ShardRoutingDataSource();
        routingDataSource.setTargetDataSources(targetDataSources);
        // set the default shard to shard1
        routingDataSource.setDefaultTargetDataSource(shard1);
        return routingDataSource;
    }
}
