package org.distributed.shardingjh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.util.EncryptUtil;
import org.distributed.shardingjh.util.SignatureUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.time.LocalDateTime;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
public class OrderControllerSecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${sha256.secret.key}")
    private String SECRET_KEY;

    private RequestOrder initialOrder;

    @BeforeEach
    public void setup() {
        initialOrder = new RequestOrder();
        initialOrder.setMemberId("test-member-001");
        initialOrder.setCreateTime(LocalDateTime.of(2025, 5, 25, 10, 0));
        initialOrder.setIsPaid(1);
        initialOrder.setPrice(500);
        initialOrder.generateOrderId();
    }

    @Test
    public void testSaveOrder_withValidSignature_shouldReturnSuccess() throws Exception {

        String bodyJson = SignatureUtil.toCanonicalJson(initialOrder, objectMapper);
        String signature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);

        mockMvc.perform(MockMvcRequestBuilders.post("/order/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bodyJson)
                        .header("X-Signature", signature))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0000")); // Assuming "0000" means success
    }

    @Test
    public void testDelete_withValidSignature_shouldReturnSuccess() throws Exception {
        String bodyJson = SignatureUtil.toCanonicalJson(initialOrder, objectMapper);
        String saveSignature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);
        String savedJson = mockMvc.perform(post("/order/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bodyJson)
                        .header("X-Signature", saveSignature))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode jsonNode = objectMapper.readTree(savedJson);
        JsonNode toDeleteJson = jsonNode.get("data");
        String dataDeleteJson = SignatureUtil.toCanonicalJson(toDeleteJson, objectMapper);;

        String signature = EncryptUtil.hmacSha256(dataDeleteJson, SECRET_KEY);
        mockMvc.perform(MockMvcRequestBuilders.post("/order/delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(dataDeleteJson)
                        .header("X-Signature", signature))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0000"));
    }

    @Test
    public void testUpdate_withValidSignature_shouldReturnSuccess() throws Exception {
        String bodyJson = SignatureUtil.toCanonicalJson(initialOrder, objectMapper);
        String saveSignature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);
        String savedJson = mockMvc.perform(post("/order/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bodyJson)
                        .header("X-Signature", saveSignature))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        log.info("Saved JSON: {}", savedJson);
        JsonNode jsonNode = objectMapper.readTree(savedJson);
        JsonNode toUpdateJson = jsonNode.get("data");
        String dataUpdateJson = SignatureUtil.toCanonicalJson(toUpdateJson, objectMapper);
        log.info("Update JSON: {}", dataUpdateJson);

        String signature = EncryptUtil.hmacSha256(dataUpdateJson, SECRET_KEY);
        mockMvc.perform(MockMvcRequestBuilders.post("/order/update")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(dataUpdateJson)
                        .header("X-Signature", signature))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0000"));
    }

    @Test
    public void testFindRange_withValidSignature_shouldReturnSuccess() throws Exception {
        String url = "/order/findRange?startDate=2025-05-01&endDate=2025-05-31";
        String signature = EncryptUtil.hmacSha256(url, SECRET_KEY);

        mockMvc.perform(MockMvcRequestBuilders.get(url)
                        .header("X-Signature", signature))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0000"));
    }

    @Test
    public void testFindRangeLocal_withValidSignature_shouldReturnSuccess() throws Exception {
        String url = "/order/findRangeLocal?startDate=2025-05-01&endDate=2025-05-31";
        String signature = EncryptUtil.hmacSha256(url, SECRET_KEY);

        mockMvc.perform(MockMvcRequestBuilders.get(url)
                        .header("X-Signature", signature))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }

    @Test
    public void testGetOne_withValidSignature_shouldReturnSuccess() throws Exception {
        String orderId = initialOrder.getOrderId();
        // First save the order to ensure it exists
        String bodyJson = SignatureUtil.toCanonicalJson(initialOrder, objectMapper);
        String saveSignature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);
        String savedJson = mockMvc.perform(post("/order/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bodyJson)
                        .header("X-Signature", saveSignature))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();


        String url = "/order/getOne?orderId="+orderId+"&createTime=2025-05-25";
        String signature = EncryptUtil.hmacSha256(url, SECRET_KEY);

        mockMvc.perform(MockMvcRequestBuilders.get(url)
                        .header("X-Signature", signature))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0000"));
    }

    @Test
    public void testHistory_withValidSignature_shouldReturnSuccess() throws Exception {
        String orderId = initialOrder.getOrderId();
        // First save the order to ensure it exists
        String bodyJson = SignatureUtil.toCanonicalJson(initialOrder, objectMapper);
        String saveSignature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);
        String savedJson = mockMvc.perform(post("/order/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bodyJson)
                        .header("X-Signature", saveSignature))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        String url = "/order/history?orderId="+orderId+"&createTime=2025-05-25";
        String signature = EncryptUtil.hmacSha256(url, SECRET_KEY);
        mockMvc.perform(MockMvcRequestBuilders.get(url)
                        .header("X-Signature", signature))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0000"));
    }

}
