package org.distributed.shardingjh;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.util.EncryptUtil;
import org.distributed.shardingjh.util.OrderSignatureUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Simulate MVCC concurrency conflict
 * Test procedure:
 * 1. Create a new order (POST /order/save)
 * 2. Read it once, duplicate the object
 * 3. Modify both versions
 * 4. Submit both updates in parallel
 * Assert:
 * 1. One update succeeds (200 OK)
 * 2. The other fails with version mismatch (DB_CONFLICT)
 *
 * */
@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
public class OrderControllerMVCCConcurrencyTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private RequestOrder initialOrder;

    @Value("${sha256.secret.key}")
    private String SECRET_KEY;

    @BeforeEach
    public void setup() {
        initialOrder = new RequestOrder();
        initialOrder.setMemberId("test-member-001");
        initialOrder.setCreateTime(LocalDateTime.of(2025, 5, 25, 10, 0));
        initialOrder.setIsPaid(1);
        initialOrder.setPrice(500);
        initialOrder.generateOrderId();
    }

    /**
     * #TODO: open firstFailureSimulated.compareAndExchange check
     * If don't open check, real world situation would both rollback
     * */
    @Test
    public void testMVCCConflictOnConcurrentUpdate() throws Exception {
        String bodyJson = OrderSignatureUtil.toCanonicalJson(initialOrder, objectMapper);
        String signature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);
        // Step 1: Save original order
        String savedJson = mockMvc.perform(post("/order/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Signature", signature)
                        .content(bodyJson))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        MgrResponseDto savedResponse = objectMapper.readValue(savedJson, MgrResponseDto.class);
        OrderTable savedOrder = objectMapper.convertValue(savedResponse.getData(), OrderTable.class);

        // Step 2: Make two copies
        OrderTable updateA = objectMapper.readValue(OrderSignatureUtil.toCanonicalJson(savedOrder, objectMapper), OrderTable.class);
        OrderTable updateB = objectMapper.readValue(OrderSignatureUtil.toCanonicalJson(savedOrder, objectMapper), OrderTable.class);
        // Modify the copies
        updateA.setPrice(111);
        updateB.setPrice(999);

        // Step 3: Send both updates in parallel
        CompletableFuture<String> resultA = CompletableFuture.supplyAsync(() -> sendUpdate(updateA));
        CompletableFuture<String> resultB = CompletableFuture.supplyAsync(() -> sendUpdate(updateB));

        String jsonA = resultA.get();
        String jsonB = resultB.get();

        MgrResponseDto resA = objectMapper.readValue(jsonA, MgrResponseDto.class);
        MgrResponseDto resB = objectMapper.readValue(jsonB, MgrResponseDto.class);
        log.info("Result A: {}, Result B: {}", resA.toString(), resB.toString());

        // Step 4: Assert one succeeded, one failed
        // 0402 is DB_CONFLICT, 0000 is SUCCESS
        boolean oneFailed = resA.getCode().equals("0402") || resB.getCode().equals("0402");
        boolean oneSucceeded = resA.getCode().equals("0000") || resB.getCode().equals("0000");

        assertThat(oneSucceeded).isTrue();
        assertThat(oneFailed).isTrue();
    }

    private String sendUpdate(OrderTable order) {
        try {
            // Generate signature for the update
            String bodyJson = OrderSignatureUtil.toCanonicalJson(order, objectMapper);
            String signature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);
            return mockMvc.perform(post("/order/update")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-Signature", signature)
                            .content(bodyJson))
                    .andReturn().getResponse().getContentAsString();
        } catch (Exception e) {
            return "{\"code\":\"ERROR\",\"message\":\"" + e.getMessage() + "\"}";
        }
    }

    // DTO helper for parsing response
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MgrResponseDto {
        private String code;
        private String message;
        private Object data;

        public String getCode() { return code; }
        public void setCode(String code) { this.code = code; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public Object getData() { return data; }
        public void setData(Object data) { this.data = data; }

        @Override
        public String toString() {
            return "MgrResponseDto{" +
                    "code='" + code + '\'' +
                    ", message='" + message + '\'' +
                    ", data=" + data +
                    '}';
        }
    }
}
