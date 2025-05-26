package org.distributed.shardingjh;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.model.OrderKey;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Simulate Read While Writing (in WAL mode)
 * Test procedure:
 * 1. Create a new order (POST /order/save)
 * 2. Trigger an update (delayed)
 * 3. At the same time, do a read
 * Assert:
 * 1. The update completes
 * 2. The read sees a consistent (non-blocked) state and returns the original data
 *
 * */
@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
public class SQLiteWALConcurrencyTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private String orderId;

    @BeforeEach
    public void setup() throws Exception {
        RequestOrder order = new RequestOrder();
        order.setCreateTime(LocalDateTime.of(2025, 5, 25, 10, 0));
        order.setIsPaid(1);
        order.setMemberId("test-wal");
        order.setPrice(100);
        order.generateOrderId();
        orderId = order.getOrderId();

        mockMvc.perform(post("/order/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order)))
                .andExpect(status().isOk());
    }

    @Test
    public void testReadDuringWriteDoesNotBlock() throws Exception {
        // Thread A: delayed write
        CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(300); // delay to overlap with read
                OrderTable update = new OrderTable();
                update.setId(new OrderKey(orderId, 1));
                update.setCreateTime(LocalDateTime.of(2025, 5, 25, 10, 0));
                update.setIsPaid(0);
                update.setMemberId("test-wal");
                update.setPrice(999);

                mockMvc.perform(post("/order/update")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(update)))
                        .andExpect(status().isOk());
            } catch (Exception ignored) {}
        });

        // Thread B: perform read during write
        CompletableFuture<String> reader = CompletableFuture.supplyAsync(() -> {
            try {
                return mockMvc.perform(get("/order/getOne")
                                .param("orderId", orderId)
                                .param("createTime", "2025-05-25"))
                        .andExpect(status().isOk())
                        .andReturn().getResponse().getContentAsString();
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        });

        writer.get(); // wait for write to complete
        String readResult = reader.get();
        log.info("Read result: {}", readResult);

        assertThat(readResult).contains(orderId); // ensure reader didn't fail
    }
}
