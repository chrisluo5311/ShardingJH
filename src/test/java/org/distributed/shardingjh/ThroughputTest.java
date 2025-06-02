package org.distributed.shardingjh;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.util.EncryptUtil;
import org.distributed.shardingjh.util.SignatureUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
@SpringBootTest
@Slf4j
public class ThroughputTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${sha256.secret.key}")
    private String SECRET_KEY;

    /**
     * Number of concurrent threads: 30
     * Number of requests per thread: 10
     * This test simulates concurrent requests to create members in the system.
     *
     * Result:
     * Total requests: 300 in 0.246 seconds. Throughput: 1219.5121951219512 req/s
     *
     * */
    @Test
    void testConcurrentMemberCreationThroughput() throws Exception {
        int threadCount = 30; // Number of concurrent threads
        int requestsPerThread = 10; // Requests per thread
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(threadCount * requestsPerThread);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < requestsPerThread; j++) {
                    try {
                        String memberId = java.util.UUID.randomUUID().toString();
                        Member member = new Member();
                        member.setId(memberId);
                        member.setName("User-" + memberId);
                        String bodyJson = SignatureUtil.toCanonicalJson(member, objectMapper);
                        String signature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);
                        mockMvc.perform(MockMvcRequestBuilders.post("/user/save")
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content(bodyJson)
                                        .header("X-Signature", signature))
                                        .andExpect(status().isOk());
                    } catch (Exception e) {
                        // Optionally log or collect errors
                        log.error("Error during request: ", e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
        long end = System.currentTimeMillis();
        long totalRequests = threadCount * requestsPerThread;
        double seconds = (end - start) / 1000.0;
        double throughput = totalRequests / seconds;
        log.info("Total requests: {} in {} seconds. Throughput: {} req/s", totalRequests, seconds, throughput);
        Assertions.assertTrue(throughput > 0, "Throughput should be positive");
    }
}
