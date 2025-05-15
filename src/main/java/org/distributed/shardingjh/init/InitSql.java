package org.distributed.shardingjh.init;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Initialize the database tables for the application.
 * This class creates the necessary tables in the database when the application starts.
 * It uses the CommandLineRunner interface to execute SQL commands after the application context is loaded.
 *
 * @author chris
 */
@Slf4j
@Component
public class InitSql implements CommandLineRunner {

    private final DataSource shardCommon1;
    private final DataSource shardCommon2;
    private final DataSource shardOrder2024;
    private final DataSource shardOrder2025;
    private final DataSource shardOrderOld;

    public InitSql(@Qualifier("shardCommon1DataSource") DataSource shardCommon1,
                    @Qualifier("shardCommon2DataSource") DataSource shardCommon2,
                    @Qualifier("shardOrder2024DataSource") DataSource shardOrder2024,
                    @Qualifier("shardOrder2025DataSource") DataSource shardOrder2025,
                    @Qualifier("shardOrderOldDataSource") DataSource shardOrderOld) {
        this.shardCommon1 = shardCommon1;
        this.shardCommon2 = shardCommon2;
        this.shardOrder2024 = shardOrder2024;
        this.shardOrder2025 = shardOrder2025;
        this.shardOrderOld = shardOrderOld;
    }

    @Override
    public void run(String... args) throws Exception {
        // user
        String createUserSql = "CREATE TABLE IF NOT EXISTS member (" +
                "id varchar(255) not null, " +
                "name varchar(255), " +
                "PRIMARY KEY (id)" +
                ");";
        // order
        String createOrderSql = "CREATE TABLE IF NOT EXISTS order_table (" +
                "order_id integer, " +
                "create_time timestamp, " +
                "is_paid integer, " +
                "PRIMARY KEY (order_id)" +
                ");";

        try (Connection conn = shardCommon1.getConnection();
                Statement stmt = conn.createStatement();
                Connection conn2 = shardCommon2.getConnection();
                Statement stmt2 = conn2.createStatement()) {
            stmt.execute(createUserSql);
            stmt2.execute(createUserSql);
        }

        try (Connection conn = shardOrder2024.getConnection();
                Statement stmt = conn.createStatement();
                Connection conn2 = shardOrder2025.getConnection();
                Statement stmt2 = conn2.createStatement();
                Connection conn3 = shardOrderOld.getConnection();
                Statement stmt3 = conn3.createStatement()) {
            stmt.execute(createOrderSql);
            stmt2.execute(createOrderSql);
            stmt3.execute(createOrderSql);
        }

        log.info("Database tables initialized successfully.");
    }
}
