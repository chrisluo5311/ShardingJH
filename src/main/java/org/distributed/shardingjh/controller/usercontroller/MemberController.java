package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.config.ServerRouter;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.p2p.FingerTable;
import org.distributed.shardingjh.service.Impl.MemberServiceImpl;
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

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Resource
    FingerTable fingerTable;

    @RequestMapping(value = "/user/save", method = RequestMethod.POST)
    public MgrResponseDto<Member> saveMember(@RequestBody Member member) {
        String responsibleUrl = serverRouter.getMemberResponsibleServerUrl(member.getId());
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            return serverRouter.forwardPost(responsibleUrl, "/user/save", member, MgrResponseDto.class);
        }
        Member newMember = memberServiceImpl.saveMember(member);
        return MgrResponseDto.success(newMember);
    }

    @RequestMapping(value = "/user/get/{id}", method = RequestMethod.GET)
    public MgrResponseDto<Member> getOneMember(@PathVariable String id) {
        String responsibleUrl = serverRouter.getMemberResponsibleServerUrl(id);
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardGet(responsibleUrl, "/user/get/" + id, MgrResponseDto.class);
        }
        Member member = memberServiceImpl.findById(id);
        if (member == null) {
            return MgrResponseDto.error(MgrResponseCode.MEMBER_NOT_FOUND);
        }
        return MgrResponseDto.success(member);
    }

    @RequestMapping(value = "/user/getAll", method = RequestMethod.GET)
    public MgrResponseDto<List<Member>> getAllMembers() {
        List<Member> all = new ArrayList<>(memberServiceImpl.findAllMembers());

        for (String node : fingerTable.finger.values()) {
            if (!node.equals(CURRENT_NODE_URL)) {
                List<Member> remote = serverRouter.forwardGetRaw(
                        node, "/user/getAllLocal", new ParameterizedTypeReference<List<Member>>() {});
                all.addAll(remote);
            }
        }
        log.info("Size of all members: {}", all.size());
        return MgrResponseDto.success(all);
    }

    @RequestMapping(value = "/user/getAllLocal", method = RequestMethod.GET)
    public List<Member> getAllMembersLocal() {
        return memberServiceImpl.findAllMembers();
    }

    @RequestMapping(value = "/user/update", method = RequestMethod.POST)
    public MgrResponseDto<Member> updateMember(@RequestBody Member member) {
        String responsibleUrl = serverRouter.getMemberResponsibleServerUrl(member.getId());
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardPost(responsibleUrl, "/user/update", member, MgrResponseDto.class);
        }
        Member updatedMember = memberServiceImpl.updateMember(member);
        if (updatedMember == null) {
            return MgrResponseDto.error(MgrResponseCode.MEMBER_NAME_INVALID);
        }
        return MgrResponseDto.success(updatedMember);
    }

    @RequestMapping(value = "/user/delete/{id}", method = RequestMethod.DELETE)
    public MgrResponseDto<String> deleteMember(@PathVariable String id) {
        String responsibleUrl = serverRouter.getMemberResponsibleServerUrl(id);
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            serverRouter.forwardDelete(responsibleUrl, "/user/delete/" + id);
            return MgrResponseDto.success("User deleted successfully");
        }
        memberServiceImpl.deleteMember(id);
        return MgrResponseDto.success("User deleted successfully");
    }
}
