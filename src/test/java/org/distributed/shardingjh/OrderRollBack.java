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
import org.springframework.test.web.servlet.MvcResult;

import java.time.LocalDateTime;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
public class OrderRollBack {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private RequestOrder initialOrder;

    @BeforeEach
    public void setup() {
        // Step 1: Create new order
        initialOrder = new RequestOrder();
        initialOrder.setMemberId("test-member-001");
        initialOrder.setCreateTime(LocalDateTime.of(2025, 5, 25, 10, 0));
        initialOrder.setIsPaid(1);
        initialOrder.setPrice(500);
        initialOrder.generateOrderId();
    }

    @Test
    public void testRollbackOnUpdateFailure() throws Exception {

        String orderId = initialOrder.getOrderId();

        mockMvc.perform(post("/order/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(initialOrder)))
                .andExpect(status().isOk());

        // Step 2: Prepare update (will be rolled back)
        OrderTable update = new OrderTable();
        update.setId(new OrderKey(orderId, 1));
        update.setCreateTime(initialOrder.getCreateTime());
        update.setIsPaid(0);
        update.setMemberId(initialOrder.getMemberId());
        update.setPrice(999);

        // Step 3: Trigger update with rollback
        try  {
            mockMvc.perform(post("/order/updateAndFail")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(update)))
                    .andExpect(status().isInternalServerError()); // 500 due to forced exception
        } catch (Exception e) {
            log.error("❌ Caught expected exception: {}", String.valueOf(e.getCause()));
        }


        // Step 4: Verify order version is still 1
        String json = mockMvc.perform(get("/order/getOne")
                        .param("orderId", orderId)
                        .param("createTime", "2025-05-25"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        log.info("Order JSON: {}", json);

        MvcResult result = mockMvc.perform(get("/order/history")
                        .param("orderId", orderId)
                        .param("createTime", "2025-05-25"))
                .andExpect(status().isOk())
                .andReturn();

        String body = result.getResponse().getContentAsString();
        log.info("Order history: {}", body);
        assertThat(body).contains("\"version\":1");
        assertThat(body).doesNotContain("\"version\":2"); // ✅ confirm rollback prevented new version
    }

}
