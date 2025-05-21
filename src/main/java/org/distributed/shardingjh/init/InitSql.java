package org.distributed.shardingjh.init;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.ShardConst;
import org.distributed.shardingjh.service.Impl.OrderIdGenerator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;
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
    private final int serverId;

    public InitSql(@Qualifier("shardCommon1DataSource") DataSource shardCommon1,
                    @Qualifier("shardCommon2DataSource") DataSource shardCommon2,
                    @Qualifier("shardOrder2024DataSource") DataSource shardOrder2024,
                    @Qualifier("shardOrder2025DataSource") DataSource shardOrder2025,
                    @Qualifier("shardOrderOldDataSource") DataSource shardOrderOld,
                    @Value("${router.server-id}") int serverId) {
        this.shardCommon1 = shardCommon1;
        this.shardCommon2 = shardCommon2;
        this.shardOrder2024 = shardOrder2024;
        this.shardOrder2025 = shardOrder2025;
        this.shardOrderOld = shardOrderOld;
        this.serverId = serverId;
    }

    @Override
    public void run(String... args) throws Exception {
        String[] names = {
                "Emma Kingston", "Liam Archer", "Olivia Ford", "Mason Blake", "Ava Carter",
                "Noah Grant", "Sophia Hayes", "Lucas Reed", "Mia Dawson", "Ethan Turner",
                "Grace Bennett", "Logan Parker", "Chloe Brooks", "Jack Sullivan", "Zoe Mitchell",
                "Henry Barrett", "Lily Foster", "Samuel Webster", "Nora Jennings", "Isaac Harper"
        };
        Random random = new Random();

        // user
        String createUserSql = "CREATE TABLE IF NOT EXISTS member (" +
                "id varchar(255) not null, " +
                "name varchar(255), " +
                "PRIMARY KEY (id)" +
                ");";
        // order
        String createOrderSql = "CREATE TABLE IF NOT EXISTS order_table (" +
                "order_id varchar(255), " +
                "version INTEGER, " +
                "create_time TIMESTAMP, " +
                "is_paid INTEGER, " +
                "member_id varchar(255)," +
                "expired_at TIMESTAMP, " +
                "is_deleted INTEGER, " +
                "PRIMARY KEY (order_id, version)" +
                ");";

        try (Connection conn = shardCommon1.getConnection();
                Statement stmt = conn.createStatement();
                Connection conn2 = shardCommon2.getConnection();
                Statement stmt2 = conn2.createStatement();
                Connection ord_conn = shardOrder2024.getConnection();
                Statement ord_stmt = ord_conn.createStatement();
                Connection ord_conn2 = shardOrder2025.getConnection();
                Statement ord_stmt2 = ord_conn2.createStatement();
                Connection ord_conn3 = shardOrderOld.getConnection();
                Statement ord_stmt3 = ord_conn3.createStatement()) {
            // Create tables in shard_common_1
            stmt.execute("DROP TABLE IF EXISTS member");
            stmt.execute(createUserSql);
            stmt.executeUpdate("DELETE FROM member");
            // Create tables in shard_common_2
            stmt2.execute("DROP TABLE IF EXISTS member");
            stmt2.execute(createUserSql);
            stmt2.executeUpdate("DELETE FROM member");
            // Create tables in shard_order_2024
            ord_stmt.execute("DROP TABLE IF EXISTS order_table");
            ord_stmt.execute(createOrderSql);
            ord_stmt.executeUpdate("DELETE FROM order_table");
            // Create tables in shard_order_2025
            ord_stmt2.execute("DROP TABLE IF EXISTS order_table");
            ord_stmt2.execute(createOrderSql);
            ord_stmt2.executeUpdate("DELETE FROM order_table");
            // Create tables in shard_order_old
            ord_stmt3.execute("DROP TABLE IF EXISTS order_table");
            ord_stmt3.execute(createOrderSql);
            ord_stmt3.executeUpdate("DELETE FROM order_table");

            int cnt_1 = 30;
            int cnt_2 = 30;
            while (cnt_1 > 0 || cnt_2 > 0) {
                String memberId = UUID.randomUUID().toString();
                int serverIndex = Math.abs(memberId.hashCode()) % ShardConst.TOTAL_SERVER_COUNT + 1;
                if (serverId == serverIndex) {
                    int shardIndex = Math.abs(memberId.hashCode()) % ShardConst.TOTAL_SHARD_COMMON_COUNT + 1;
                    String randomMemberName = names[random.nextInt(names.length)];
                    if (shardIndex == 1 && cnt_1 > 0) {
                        // Insert member into shard_common_1
                        cnt_1 -= 1;
                        stmt.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberId+"', '"+randomMemberName+"')");
                        LocalDateTime date1 = LocalDate.of(2025, 3, random.nextInt(30)+1).atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
                        String orderId1 = OrderIdGenerator.generateOrderId(date1, memberId);
                        ord_stmt2.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, version, expired_at, is_deleted) " +
                                "VALUES ('"+orderId1+"' ,'"+date1.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberId+"', 1, null, 0)");
                        LocalDateTime date2 = LocalDate.of(2024, 3, random.nextInt(30)+1).atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
                        String orderId2 = OrderIdGenerator.generateOrderId(date2, memberId);
                        ord_stmt.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, version, expired_at, is_deleted) " +
                                "VALUES ('"+orderId2+"' ,'"+date2.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 0, '"+memberId+"', 1, null, 0)");
                        LocalDateTime date3 = LocalDate.of(2023, 3, random.nextInt(30)+1).atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
                        String orderId3 = OrderIdGenerator.generateOrderId(date3, memberId);
                        ord_stmt3.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, version, expired_at, is_deleted) " +
                                "VALUES ('"+orderId3+"' ,'"+date3.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberId+"', 1, null, 0)");
                    } else if (shardIndex == 2 && cnt_2 > 0){
                        // Insert member into shard_common_2
                        cnt_2 -= 1;
                        stmt2.executeUpdate("INSERT INTO member (id, name) VALUES ('"+memberId+"', '"+randomMemberName+"')");
                        LocalDateTime date1 = LocalDate.of(2025, 3, random.nextInt(30)+1).atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
                        String orderId1 = OrderIdGenerator.generateOrderId(date1, memberId);
                        ord_stmt2.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, version, expired_at, is_deleted) " +
                                "VALUES ('"+orderId1+"' ,'"+date1.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberId+"', 1, null, 0)");
                        LocalDateTime date2 = LocalDate.of(2024, 3, random.nextInt(30)+1).atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
                        String orderId2 = OrderIdGenerator.generateOrderId(date2, memberId);
                        ord_stmt.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, version, expired_at, is_deleted) " +
                                "VALUES ('"+orderId2+"' ,'"+date2.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 0, '"+memberId+"', 1, null, 0)");
                        LocalDateTime date3 = LocalDate.of(2023, 3, random.nextInt(30)+1).atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
                        String orderId3 = OrderIdGenerator.generateOrderId(date3, memberId);
                        ord_stmt3.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, version, expired_at, is_deleted) " +
                                "VALUES ('"+orderId3+"' ,'"+date3.toInstant(ZoneOffset.UTC).toEpochMilli()+"', 1, '"+memberId+"', 1, null, 0)");
                    }
                }
            }
        }

        log.info("Database tables initialized successfully.");
    }
}