package org.distributed.shardingjh;

import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.service.Impl.OrderIdGenerator;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for OrderIdGenerator.
 */
@Slf4j
public class OrderIdGeneratorTest {

    @Test
    public void testGenerateOrderId_NotNullOrEmpty() throws Exception {
        String input = "2024-06-11 14:30:45";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(input, formatter);

        String memberId = "user123";
        String orderId = OrderIdGenerator.generateOrderId(dateTime, memberId);

        log.info("Generated Order ID: {}", orderId);
        assertNotNull(orderId, "Order ID should not be null");
        assertFalse(orderId.isEmpty(), "Order ID should not be empty");
        assertEquals(32, orderId.length(), "Order ID (MD5) should be 32 characters long");
    }

    @Test
    public void testGenerateOrderId_DifferentInputsProduceDifferentIds() throws Exception {
        String input = "2024-06-11 14:30:45";
        String laterInput = "2024-06-11 14:31:00";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(input, formatter);
        LocalDateTime laterDateTime = LocalDateTime.parse(laterInput, formatter);

        String memberId1 = "user123";
        String memberId2 = "user456";

        String id1 = OrderIdGenerator.generateOrderId(dateTime, memberId1);
        String id2 = OrderIdGenerator.generateOrderId(dateTime, memberId2);
        String id3 = OrderIdGenerator.generateOrderId(laterDateTime, memberId1);

        log.info("Generated Order ID 1: {}", id1);
        log.info("Generated Order ID 2: {}", id2);
        log.info("Generated Order ID 3: {}", id3);
        assertNotEquals(id1, id2, "Different memberIds should generate different orderIds");
        assertNotEquals(id1, id3, "Different createTimes should generate different orderIds");
    }
}