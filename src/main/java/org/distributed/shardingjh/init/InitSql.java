package org.distributed.shardingjh.init;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.ShardConst;
import org.distributed.shardingjh.service.Impl.OrderIdGenerator;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
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
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Initialize the database tables for the application.
 * This class creates the necessary tables in the database when the application starts.
 * It uses the CommandLineRunner interface to execute SQL commands after the application context is loaded.
 * Total 5 databases are created:
 * 1. shard_common_1: Contains member table
 * 2. shard_common_2: Contains member table
 * 3. shard_order_2024: Contains order_table for 2024
 * 4. shard_order_2025: Contains order_table for 2025
 * 5. shard_order_old: Contains order_table for old orders
 *
 * @author chris
 */
@Slf4j
@Component
public class InitSql implements CommandLineRunner {

    @Resource
    RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry;

    private final DataSource shardCommon1;
    private final DataSource shardCommon2;
    private final DataSource shardOrder2024;
    private final DataSource shardOrder2025;
    private final DataSource shardOrderOld;
    private final DataSource productDatabase;
    private final String CURRENT_NODE_URL;

    public InitSql(@Qualifier("shardCommon1DataSource") DataSource shardCommon1,
                    @Qualifier("shardCommon2DataSource") DataSource shardCommon2,
                    @Qualifier("shardOrder2024DataSource") DataSource shardOrder2024,
                    @Qualifier("shardOrder2025DataSource") DataSource shardOrder2025,
                    @Qualifier("shardOrderOldDataSource") DataSource shardOrderOld,
                    @Qualifier("productDataSource") DataSource productDatabase,
                    @Value("${router.server-url}") String CURRENT_NODE_URL) {
        this.shardCommon1 = shardCommon1;
        this.shardCommon2 = shardCommon2;
        this.shardOrder2024 = shardOrder2024;
        this.shardOrder2025 = shardOrder2025;
        this.shardOrderOld = shardOrderOld;
        this.productDatabase = productDatabase;
        this.CURRENT_NODE_URL = CURRENT_NODE_URL;
    }

    @Override
    public void run(String... args) throws Exception {
        String[] names = {
                "Emma Kingston", "Liam Archer", "Olivia Ford", "Mason Blake", "Ava Carter",
                "Noah Grant", "Sophia Hayes", "Lucas Reed", "Mia Dawson", "Ethan Turner",
                "Grace Bennett", "Logan Parker", "Chloe Brooks", "Jack Sullivan", "Zoe Mitchell",
                "Henry Barrett", "Lily Foster", "Samuel Webster", "Nora Jennings", "Isaac Harper",
                "Charlotte Murphy", "Alexander Cooper", "Amelia Rivera", "James Morgan", "Ella Bell",
                "Benjamin Ward", "Sofia Price", "Daniel Wood", "Avery Ross", "Matthew Hughes",
                "Scarlett Powell", "David Rivera", "Aria Long", "Joseph Reed", "Madison Diaz",
                "Michael Cox", "Layla Sanders", "William Perry", "Samantha Ramirez", "Jackson Butler",
        };
        Random random = new Random();

        // member table
        String createUserSql = "CREATE TABLE IF NOT EXISTS member (" +
                "id varchar(255) not null, " +
                "name varchar(255), " +
                "PRIMARY KEY (id)" +
                ");";
        // order table
        String createOrderSql = "CREATE TABLE IF NOT EXISTS order_table (" +
                "order_id varchar(255), " +
                "version INTEGER, " +
                "create_time TIMESTAMP, " +
                "is_paid INTEGER, " +
                "member_id varchar(255)," +
                "price INTEGER, " +
                "expired_at TIMESTAMP, " +
                "is_deleted INTEGER, " +
                "PRIMARY KEY (order_id, version)" +
                ");";

        // product table
        String createProductSql = "CREATE TABLE IF NOT EXISTS product (" +
                "id varchar(255) PRIMARY KEY, " +
                "name varchar(255), " +
                "price INTEGER " +
                ");";

        // Get connections to the databases
        try (Connection conn = shardCommon1.getConnection();
                Statement stmt = conn.createStatement();
                Connection conn2 = shardCommon2.getConnection();
                Statement stmt2 = conn2.createStatement();
                Connection ord_conn = shardOrder2024.getConnection();
                Statement ord_stmt = ord_conn.createStatement();
                Connection ord_conn2 = shardOrder2025.getConnection();
                Statement ord_stmt2 = ord_conn2.createStatement();
                Connection ord_conn3 = shardOrderOld.getConnection();
                Statement ord_stmt3 = ord_conn3.createStatement();
                Connection prod_conn = productDatabase.getConnection();
                Statement prod_stmt = prod_conn.createStatement()) {

            // ✅ Enable WAL mode
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt2.execute("PRAGMA journal_mode=WAL");
            ord_stmt.execute("PRAGMA journal_mode=WAL");
            ord_stmt2.execute("PRAGMA journal_mode=WAL");
            ord_stmt3.execute("PRAGMA journal_mode=WAL");
            prod_stmt.execute("PRAGMA journal_mode=WAL");

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

            // Create tables in product database
            prod_stmt.execute("DROP TABLE IF EXISTS product");
            prod_stmt.execute(createProductSql);
            prod_stmt.executeUpdate("DELETE FROM product");

            int memberInserted1 = 0;
            int memberInserted2 = 0;
            int order2025Inserted = 0;
            int order2024Inserted = 0;
            int order2023Inserted = 0;
            int maxDataSize = 30;

            TreeMap<Integer, String> serverUrls = new TreeMap<>(Map.of(
                64, "http://18.222.111.89:8081",
                128, "http://3.15.149.110:8082",
                192, "http://52.15.151.104:8083",
                224, "http://3.22.61.73:8084"
            ));

//            TreeMap<Integer, String> serverUrls = new TreeMap<>(Map.of(
//                    64, "http://localhost:8081",
//                    128, "http://localhost:8082",
//                    192, "http://localhost:8083"
//            ));

            while (memberInserted1 < maxDataSize || memberInserted2 < maxDataSize || order2025Inserted < maxDataSize ||
                    order2024Inserted < maxDataSize || order2023Inserted < maxDataSize) {
                String memberId = UUID.randomUUID().toString();
                int shardIndex = Math.abs(memberId.hashCode()) % ShardConst.TOTAL_SHARD_COMMON_COUNT + 1;
                String randomMemberName = names[random.nextInt(names.length)];

                // Order dates
                LocalDateTime date2025 = LocalDate.of(2025, random.nextInt(12)+1, random.nextInt(28) + 1)
                    .atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
                LocalDateTime date2024 = LocalDate.of(2024, random.nextInt(12)+1, random.nextInt(28) + 1)
                    .atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
                LocalDateTime date2023 = LocalDate.of(2023, random.nextInt(12)+1, random.nextInt(28) + 1)
                    .atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));

                String orderId2025 = OrderIdGenerator.generateOrderId(date2025, memberId);
                String orderId2024 = OrderIdGenerator.generateOrderId(date2024, memberId);
                String orderId2023 = OrderIdGenerator.generateOrderId(date2023, memberId);

                // MEMBER shard 1 INSERTION
                if (memberInserted1 < maxDataSize && isResponsible(memberId, serverUrls, CURRENT_NODE_URL)) {
                    if (shardIndex == 1) {
                        stmt.executeUpdate("INSERT INTO member (id, name) VALUES ('" + memberId + "', '" + randomMemberName + "')");
                        memberInserted1++;
                    }
                }
                // MEMBER shard 2 INSERTION
                if (memberInserted2 < maxDataSize && isResponsible(memberId, serverUrls, CURRENT_NODE_URL)) {
                    if (shardIndex == 2) {
                        stmt2.executeUpdate("INSERT INTO member (id, name) VALUES ('" + memberId + "', '" + randomMemberName + "')");
                        memberInserted2++;
                    }
                }

                // ORDER 2025
                if (order2025Inserted < maxDataSize && isResponsible(orderId2025, serverUrls, CURRENT_NODE_URL)) {
                    int randomPrice = random.nextInt(1000) + 1;
                    ord_stmt2.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, price, version, expired_at, is_deleted) " +
                            "VALUES ('" + orderId2025 + "' ,'" + date2025.toInstant(ZoneOffset.UTC).toEpochMilli() + "', 1, '" +
                            memberId + "', "+randomPrice+" , 1, null, 0)");
                    order2025Inserted++;
                }

                // ORDER 2024
                if (order2024Inserted < maxDataSize && isResponsible(orderId2024, serverUrls, CURRENT_NODE_URL)) {
                    int randomPrice = random.nextInt(1000) + 1;
                    ord_stmt.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, price, version, expired_at, is_deleted) " +
                            "VALUES ('" + orderId2024 + "' ,'" + date2024.toInstant(ZoneOffset.UTC).toEpochMilli() + "', 0, '" +
                            memberId + "', "+randomPrice+", 1, null, 0)");
                    order2024Inserted++;
                }

                // ORDER 2023
                if (order2023Inserted < maxDataSize && isResponsible(orderId2023, serverUrls, CURRENT_NODE_URL)) {
                    int randomPrice = random.nextInt(1000) + 1;
                    ord_stmt3.executeUpdate("INSERT INTO order_table (order_id, create_time, is_paid, member_id, price, version, expired_at, is_deleted) " +
                            "VALUES ('" + orderId2023 + "' ,'" + date2023.toInstant(ZoneOffset.UTC).toEpochMilli() + "', 1, '" +
                            memberId + "', "+randomPrice+", 1, null, 0)");
                    order2023Inserted++;
                }
            }

            // Insert sample products
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('01b2f99e-88d0-4b1a-90c9-f2f794223c8f','Laptop' , '1000')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('72a063bc-f939-41bb-9597-4ff1069fc9c6','Smartphone' , '800')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('d5a7e55a-6adc-40bf-8ff3-6e08eac7914f','Tablet' , '600')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('d54485a9-70b4-47be-9b6b-7545c4bc74c5','Smartwatch' , '300')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('52b2d1aa-2c6a-466f-bf29-ed8a06519ca4','Headphones' , '150')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('74716f11-6a88-4ab9-b7b7-86181715811e','Bluetooth Speaker' , '100')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('1a24ddd0-ec18-42f0-84ef-0f3929801cc2','Wireless Mouse' , '50')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('bc2426cb-65d4-423b-a467-0f69994dc6c8','Keyboard' , '80')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('3eaf6e55-c64b-4af4-80ef-7cd83931567e','Monitor' , '200')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('8d0895df-12cc-43d1-b10c-e468afd3047a','Printer' , '250')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('560b9180-c954-43ae-9889-670bec04dbbc','External Hard Drive' , '120')");
            prod_stmt.executeUpdate("INSERT INTO product (id, name, price) " +
                    "VALUES ('438b4970-29a0-48da-88f8-4ec28b005113','USB Flash Drive' , '30')");
        }
        log.info("Database tables initialized successfully.");
        rabbitListenerEndpointRegistry.start();
    }

    private boolean isResponsible(String key, TreeMap<Integer, String> serverUrls, String currentNodeUrl) {
        int target = Math.abs(key.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        Map.Entry<Integer, String> entry = serverUrls.ceilingEntry(target);
        String responsibleUrl = entry != null ? entry.getValue() : serverUrls.firstEntry().getValue();
        return responsibleUrl.equals(currentNodeUrl);
    }
}