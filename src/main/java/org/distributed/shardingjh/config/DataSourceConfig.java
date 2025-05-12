package org.distributed.shardingjh.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Configures two data source beans `shard1DataSource` and `shard2DataSource`
 * with properties prefixed with `spring.datasource.shard1` and `spring.datasource.shard2`
 * @author chris
 * */
@Configuration
public class DataSourceConfig {

    @Bean(name = "shard1DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.shard1")
    public DataSource shard1DataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "shard2DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.shard2")
    public DataSource shard2DataSource() {
        return DataSourceBuilder.create().build();
    }
}
