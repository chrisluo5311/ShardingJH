package org.distributed.shardingjh.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class JdbcTemplateConfig {

    @Bean(name = "order2025JdbcTemplate")
    public JdbcTemplate jdbcTemplate2025(@Qualifier("shardOrder2025DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "order2024JdbcTemplate")
    public JdbcTemplate jdbcTemplate2024(@Qualifier("shardOrder2024DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "orderOldJdbcTemplate")
    public JdbcTemplate jdbcTemplateOld(@Qualifier("shardOrderOldDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
