package org.distributed.shardingjh;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

@ConfigurationPropertiesScan
//@PropertySources({
//        @PropertySource("classpath:shard.properties"),
//    @PropertySource("classpath:application-server1.properties"),
//    @PropertySource("classpath:application-server2.properties"),
//    @PropertySource("classpath:application-server3.properties"),
//})
@PropertySource("classpath:shard.properties")
@SpringBootApplication
public class ShardingJhApplication {

    public static void main(String[] args) {

        // To enable the different server properties
        // run "mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=server1""
        // run "mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=server2""
        // run "mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=server3""
        SpringApplication.run(ShardingJhApplication.class, args);
    }

}
