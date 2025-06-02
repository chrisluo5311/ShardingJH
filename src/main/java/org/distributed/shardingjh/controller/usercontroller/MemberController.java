package org.distributed.shardingjh.controller.usercontroller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.config.ServerRouter;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.p2p.FingerTable;
import org.distributed.shardingjh.service.Impl.MemberServiceImpl;
import org.distributed.shardingjh.util.EncryptUtil;
import org.distributed.shardingjh.util.SignatureUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Default coordinator server is 0
 * Forwarding requests to the correct server based on the id
 * */
@Slf4j
@RestController
public class MemberController {

    @Resource
    MemberServiceImpl memberServiceImpl;

    @Resource
    ServerRouter serverRouter;

    @Resource
    private ObjectMapper objectMapper;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Value("${sha256.secret.key}")
    private String SECRET_KEY;

    @Resource
    FingerTable fingerTable;

    @RequestMapping(value = "/user/save", method = RequestMethod.POST)
    public MgrResponseDto<Member> saveMember(@RequestBody Member member,
                                             @RequestHeader(value = "X-Signature") String signature) throws JsonProcessingException {
        long startLocal = System.nanoTime();
        // Check signature
        String rawBody = SignatureUtil.toCanonicalJson(member, objectMapper);
        String expectedSignature = EncryptUtil.hmacSha256(rawBody, SECRET_KEY);
        log.info("Expected signature: {}, X-Signature: {}", expectedSignature, signature);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for member: {}", member.getId());
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }

        String responsibleUrl = serverRouter.getMemberResponsibleServerUrl(member.getId());
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            try {
                log.info("Forwarding [saveMember] request to responsible server: {}", responsibleUrl);
                return serverRouter.forwardPost(responsibleUrl, "/user/save", signature, member, MgrResponseDto.class);
            } catch (JsonProcessingException e) {
                return  MgrResponseDto.error(MgrResponseCode.JSON_PARSE_ERROR);
            }
        }
        Member newMember = memberServiceImpl.saveMember(member);
        long endLocal = System.nanoTime();
        long localLatency = endLocal - startLocal;
        log.info("Local [save Member] latency: {} ns", localLatency);
        return MgrResponseDto.success(newMember);
    }

    @RequestMapping(value = "/user/get/{id}", method = RequestMethod.GET)
    public MgrResponseDto<Member> getOneMember(@PathVariable String id, @RequestHeader(value = "X-Signature") String signature) {
        //check signature
        String endPointPath = "/user/get/" + id;
        String expectedSignature = EncryptUtil.hmacSha256(endPointPath, SECRET_KEY);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for user query: user={}", id);
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }

        String responsibleUrl = serverRouter.getMemberResponsibleServerUrl(id);
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardGet(responsibleUrl, "/user/get/" + id, signature, MgrResponseDto.class);
        }
        Member member = memberServiceImpl.findById(id);
        if (member == null) {
            return MgrResponseDto.error(MgrResponseCode.MEMBER_NOT_FOUND);
        }
        return MgrResponseDto.success(member);
    }

    @RequestMapping(value = "/user/getAll", method = RequestMethod.GET)
    public MgrResponseDto<List<Member>> getAllMembers(@RequestHeader(value = "X-Signature") String signature) {
        long startLocal = System.nanoTime();
        // Check signature
        String endPointPath = "/user/getAll";
        String expectedSignature = EncryptUtil.hmacSha256(endPointPath, SECRET_KEY);
        log.info("Expected signature: {}, X-Signature: {}", expectedSignature, signature);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for get all members request");
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }


        List<Member> all = new ArrayList<>(memberServiceImpl.findAllMembers());
        for (String node : fingerTable.finger.values()) {
            if (!node.equals(CURRENT_NODE_URL)) {
                List<Member> remote = serverRouter.forwardGetRaw(
                        node, "/user/getAllLocal", signature, new ParameterizedTypeReference<List<Member>>() {});
                if (remote == null || remote.isEmpty()) {
                    log.warn("No members found on node: {}", node);
                    continue;
                }
                all.addAll(remote);
            }
        }
        log.info("Size of all members: {}", all.size());
        long endLocal = System.nanoTime();
        long localLatency = endLocal - startLocal;
        log.info("Local [get all members] latency: {} ns", localLatency);
        return MgrResponseDto.success(all);
    }

    /**
     * Internal endpoint for testing
     * */
    @RequestMapping(value = "/user/getAllLocal", method = RequestMethod.GET)
    public List<Member> getAllMembersLocal(@RequestHeader(value = "X-Signature", required = false) String signature) {
        return memberServiceImpl.findAllMembers();
    }

    @RequestMapping(value = "/user/update", method = RequestMethod.POST)
    public MgrResponseDto<Member> updateMember(@RequestBody Member member,
                                               @RequestHeader(value = "X-Signature") String signature) throws JsonProcessingException {
        long startLocal = System.nanoTime();
        // Check signature
        String rawBody = SignatureUtil.toCanonicalJson(member, objectMapper);
        String expectedSignature = EncryptUtil.hmacSha256(rawBody, SECRET_KEY);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for user update: user={}", member.getId());
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }
        // Determine the responsible server for this member
        String responsibleUrl = serverRouter.getMemberResponsibleServerUrl(member.getId());
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            try {
                log.info("Forwarding [updateMember] request to responsible server: {}", responsibleUrl);
                return serverRouter.forwardPost(responsibleUrl, "/user/update", signature, member, MgrResponseDto.class);
            } catch (JsonProcessingException e) {
                return  MgrResponseDto.error(MgrResponseCode.JSON_PARSE_ERROR);
            }
        }
        Member updatedMember = memberServiceImpl.updateMember(member);
        if (updatedMember == null) {
            return MgrResponseDto.error(MgrResponseCode.MEMBER_NAME_INVALID);
        }
        long endLocal = System.nanoTime();
        long localLatency = endLocal - startLocal;
        log.info("Local [update member] latency: {} ns", localLatency);
        return MgrResponseDto.success(updatedMember);
    }

    @RequestMapping(value = "/user/delete/{id}", method = RequestMethod.DELETE)
    public MgrResponseDto<String> deleteMember(@PathVariable String id,
                                               @RequestHeader(value = "X-Signature") String signature) throws JsonProcessingException {
        // Check signature
        String endPointPath = "/user/delete/" + id;
        String expectedSignature = EncryptUtil.hmacSha256(endPointPath, SECRET_KEY);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for user delete: user={}", id);
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }

        String responsibleUrl = serverRouter.getMemberResponsibleServerUrl(id);
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            serverRouter.forwardDelete(responsibleUrl, "/user/delete/" + id, signature);
            return MgrResponseDto.success("User deleted successfully");
        }
        memberServiceImpl.deleteMember(id);
        return MgrResponseDto.success("User deleted successfully");
    }
}
