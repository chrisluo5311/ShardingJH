package org.distributed.shardingjh.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Configure 5 data source beans
 * - shardCommon1DataSource
 * - shardCommon2DataSource
 * - shardOrder2024DataSource
 * - shardOrder2025DataSource
 * - shardOrderOldDataSource
 *
 * @author chris
 * */
@Configuration
public class DataSourceConfig {

    @Bean(name = "shardCommon1DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.shardcommon1")
    public DataSource shardCommon1DataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "shardCommon2DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.shardcommon2")
    public DataSource shardCommon2DataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "shardOrder2024DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.shardorder2024")
    public DataSource shardOrder2024DataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "shardOrder2025DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.shardorder2025")
    public DataSource shardOrder2025DataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "shardOrderOldDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.shardorderold")
    public DataSource shardOrderOldDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "productDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.product")
    public DataSource productDataSource() {
        return DataSourceBuilder.create().build();
    }
}
