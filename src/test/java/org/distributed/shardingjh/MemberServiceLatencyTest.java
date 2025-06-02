package org.distributed.shardingjh;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.ShardConst;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.service.MemberService;
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

import java.util.UUID;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * To run this test,
 * using this command:
 * "mvn -Dtest=SeosSdkAutomationApplicationTests test"
 *
 * */
@AutoConfigureMockMvc
@SpringBootTest
@Slf4j
public class MemberServiceLatencyTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${sha256.secret.key}")
    private String SECRET_KEY;

    @Autowired
    private MemberService memberService;

    private Member localMember;
    private Member remoteMember;

    @Test
    public void testAddMemberLatency() throws Exception {
        // IDs should be chosen so that one hashes to local, one to remote
        String localId = UUID.randomUUID().toString();
        String remoteId = UUID.randomUUID().toString();
        int localHash = Math.abs(localId.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        while (localHash > 64) {
            localId = UUID.randomUUID().toString();
            localHash = Math.abs(localId.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        }
        log.info("Local ID: {}, Hash:{}", localId, localHash);

        int remoteHash = Math.abs(remoteId.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        while (remoteHash <= 64) {
            remoteId = UUID.randomUUID().toString();
            remoteHash = Math.abs(remoteId.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        }
        log.info("Remote ID: {}, Hash:{}", remoteId, remoteHash);
        localMember = new Member();
        localMember.setId(localId);
        localMember.setName("Local User");
        log.info("Local member: {}", localMember);

        remoteMember = new Member();
        remoteMember.setId(remoteId);
        remoteMember.setName("Remote User");
        log.info("Remote member: {}", remoteMember);

        // local member
        String bodyJson = SignatureUtil.toCanonicalJson(localMember, objectMapper);
        String signature = EncryptUtil.hmacSha256(bodyJson, SECRET_KEY);
        long startLocal = System.nanoTime();
        mockMvc.perform(MockMvcRequestBuilders.post("/user/save")
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .content(bodyJson)
                                    .header("X-Signature", signature))
                            .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0000"));
        long endLocal = System.nanoTime();
        long localLatency = endLocal - startLocal;

        // remote member
        String remoteJson = SignatureUtil.toCanonicalJson(remoteMember, objectMapper);
        String remoteSignature = EncryptUtil.hmacSha256(remoteJson, SECRET_KEY);
        long startRemote = System.nanoTime();
        mockMvc.perform(MockMvcRequestBuilders.post("/user/save")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(remoteJson)
                        .header("X-Signature", remoteSignature))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0000"));
        long endRemote = System.nanoTime();

        // Calculate latencies
        long remoteLatency = endRemote - startRemote;

        long diffMillis = Math.abs((remoteLatency - localLatency) / 1_000_000);
        System.out.println("Local latency (ms): " + localLatency / 1_000_000);
        System.out.println("Remote latency (ms): " + remoteLatency / 1_000_000);
        System.out.println("Difference (ms): " + diffMillis + ", should be marginal (<5ms)");

        // Assert that the difference is marginal (e.g., less than 5ms)
        Assertions.assertTrue(diffMillis < 5, "Latency difference should be marginal (<5ms)");
    }
}
