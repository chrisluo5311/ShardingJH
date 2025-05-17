package org.distributed.shardingjh.service.Impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;

@Slf4j
@Component
public class OrderIdGenerator {
    /**
     * Generates a unique order ID based on the current time and member ID.
     *
     * @param createTime The time the order was created.
     * @param memberId   The ID of the member placing the order.
     * @return A unique order ID.
     */
    public static String generateOrderId(LocalDateTime createTime, String memberId) {
        String prefix = "order";
        String baseString = prefix + createTime.toString() + memberId;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(baseString.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            // You could return sb.toString(); (full hash) or a substring if you want it shorter
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}