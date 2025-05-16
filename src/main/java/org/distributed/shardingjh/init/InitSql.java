package org.distributed.shardingjh.init;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.service.Impl.OrderIdGenerator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

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
        String memberID1 = UUID.randomUUID().toString();
        String memberID2 = UUID.randomUUID().toString();
        String memberID3 = UUID.randomUUID().toString();
        String memberID4 = UUID.randomUUID().toString();
        String memberID5 = UUID.randomUUID().toString();
        String memberID6 = UUID.randomUUID().toString();
        String memberID7 = UUID.randomUUID().toString();
        String memberID8 = UUID.randomUUID().toString();
        String memberID9 = UUID.randomUUID().toString();
        String memberID10 = UUID.randomUUID().toString();

        // user
        String createUserSql = "CREATE TABLE IF NOT EXISTS member (" +
                "id varchar(255) not null, " +
                "name varchar(255), " +
                "PRIMARY KEY (id)" +
                ");";
        // order
        String createOrderSql = "CREATE TABLE IF NOT EXISTS order_table (" +
                "order_id varchar(255) PRIMARY KEY, " +
                "create_time TIMESTAMP, " +
                "is_paid INTEGER, " +
                "member_id varchar(255)" +
                ");";

        try (Connection conn = shardCommon1.getConnection();
                Statement stmt = conn.createStatement();
                Connection conn2 = shardCommon2.getConnection();
                Statement stmt2 = conn2.createStatement()) {
            stmt.execute(createUserSql);
            // Drop all the data in the table first
            stmt.executeUpdate("DELETE FROM member");
            stmt.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID1+"', 'Alice')");
            stmt.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID2+"', 'Bob')");
            stmt.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID3+"', 'Charlie')");
            stmt.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID4+"', 'David')");
            stmt.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID5+"', 'Eve')");

            stmt2.execute(createUserSql);
            stmt2.executeUpdate("DELETE FROM member");
            stmt2.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID6+"', 'Tom')");
            stmt2.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID7+"', 'Jerry')");
            stmt2.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID8+"', 'Peter')");
            stmt2.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID9+"', 'Daniel')");
            stmt2.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberID10+"', 'Cathy')");
        }

        try (Connection conn = shardOrder2024.getConnection();
                Statement stmt = conn.createStatement();
                Connection conn2 = shardOrder2025.getConnection();
                Statement stmt2 = conn2.createStatement();
                Connection conn3 = shardOrderOld.getConnection();
                Statement stmt3 = conn3.createStatement()) {
            stmt.execute(createOrderSql);
            stmt.executeUpdate("DELETE FROM order_table");
            LocalDateTime date1 = LocalDate.of(2024, 6, 1).atStartOfDay();
            LocalDateTime date2 = LocalDate.of(2024, 6, 2).atStartOfDay();
            LocalDateTime date3 = LocalDate.of(2024, 6, 3).atStartOfDay();
            LocalDateTime date4 = LocalDate.of(2024, 6, 4).atStartOfDay();
            String orderId1 = OrderIdGenerator.generateOrderId(date1, memberID1);
            String orderId2 =OrderIdGenerator.generateOrderId(date2, memberID2);
            String orderId3 =OrderIdGenerator.generateOrderId(date3, memberID6);
            String orderId4 =OrderIdGenerator.generateOrderId(date4, memberID7);
            stmt.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId1+"' ,'"+date1.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberID1+"')");
            stmt.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId2+"', '"+date2.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 0, '"+memberID2+"')");
            stmt.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId3+"','"+date3.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberID6+"')");
            stmt.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId4+"','"+date4.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberID7+"')");

            stmt2.execute(createOrderSql);
            stmt2.executeUpdate("DELETE FROM order_table");
            LocalDateTime date_2024_1 = LocalDate.of(2025, 6, 3).atStartOfDay();
            LocalDateTime date_2024_2 = LocalDate.of(2025, 6, 4).atStartOfDay();
            LocalDateTime date_2024_3 = LocalDate.of(2025, 6, 5).atStartOfDay();
            LocalDateTime date_2024_4 = LocalDate.of(2025, 6, 6).atStartOfDay();
            String orderId_2024_1 = OrderIdGenerator.generateOrderId(date_2024_1, memberID1);
            String orderId_2024_2 =OrderIdGenerator.generateOrderId(date_2024_2, memberID2);
            String orderId_2024_3 =OrderIdGenerator.generateOrderId(date_2024_3, memberID6);
            String orderId_2024_4 =OrderIdGenerator.generateOrderId(date_2024_4, memberID7);
            stmt2.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId_2024_1+"','"+date_2024_1.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberID3+"')");
            stmt2.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId_2024_2+"','"+date_2024_2.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 0, '"+memberID4+"')");
            stmt2.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId_2024_3+"','"+date_2024_3.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 0, '"+memberID8+"')");
            stmt2.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId_2024_4+"','"+date_2024_4.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberID9+"')");

            stmt3.execute(createOrderSql);
            stmt3.executeUpdate("DELETE FROM order_table");
            LocalDateTime date_2023_1 = LocalDate.of(2023, 6, 5).atStartOfDay();
            LocalDateTime date_2023_2 = LocalDate.of(2023, 6, 6).atStartOfDay();
            String orderId_2023_1 = OrderIdGenerator.generateOrderId(date_2023_1, memberID1);
            String orderId_2023_2 =OrderIdGenerator.generateOrderId(date_2023_2, memberID2);
            stmt3.executeUpdate("INSERT INTO order_table (order_id,create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId_2023_1+"','"+date_2023_1.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberID5+"')");
            stmt3.executeUpdate("INSERT INTO order_table (order_id,create_time, is_paid, member_id) " +
                    "VALUES ('"+orderId_2023_2+"','"+date_2023_2.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 0, '"+memberID10+"')");

        }

        log.info("Database tables initialized successfully.");
    }
}