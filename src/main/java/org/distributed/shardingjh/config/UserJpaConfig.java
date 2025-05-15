package org.distributed.shardingjh.config;

import jakarta.persistence.EntityManagerFactory;
import org.distributed.shardingjh.model.Member;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Set up the entity manager factory and JPA configuration
 * @author chris
 * */
@Configuration
@EnableJpaRepositories(
        basePackages = "org.distributed.shardingjh.repository.user", // repositories using User
        entityManagerFactoryRef = "userEntityManagerFactory",
        transactionManagerRef = "userTransactionManager"
)
public class UserJpaConfig {

    @Bean(name = "userEntityManagerFactory")
    @Primary
    public LocalContainerEntityManagerFactoryBean userEntityManagerFactory(
            @Qualifier("shardRoutingDataSource") DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan(Member.class.getPackageName());
        em.setPersistenceUnitName("shardingCommon");
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);
        em.setJpaPropertyMap(jpaProperties());

        return em;
    }

    @Bean(name = "userTransactionManager")
    @Primary
    public PlatformTransactionManager userTransactionManager(
            @Qualifier("userEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    private Map<String, Object> jpaProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("hibernate.hbm2ddl.auto", "none");
        props.put("hibernate.dialect", "org.hibernate.community.dialect.SQLiteDialect");
        return props;
    }
}
