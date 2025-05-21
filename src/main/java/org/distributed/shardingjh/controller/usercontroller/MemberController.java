package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.config.ServerRouter;
import org.distributed.shardingjh.model.Member;
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

    @Value("${router.server-id}")
    private int SERVER_ID;

    @RequestMapping(value = "/user/save", method = RequestMethod.POST)
    public MgrResponseDto<Member> saveMember(@RequestBody Member member) {
        if (!isLocal(member.getId())) {
            // forward the request to the correct server
            return serverRouter.forwardPost(member.getId(), "/user/save", member, MgrResponseDto.class);
        }
        log.info("Server id Matched!");
        Member newMember = memberServiceImpl.saveMember(member);
        return MgrResponseDto.success(newMember);
    }

    @RequestMapping(value = "/user/get/{id}", method = RequestMethod.GET)
    public MgrResponseDto<Member> getOneMember(@PathVariable String id) {
        if (!isLocal(id)) {
            // forward the request to the correct server
            return serverRouter.forwardGet(id, "/user/get/" + id, MgrResponseDto.class);
        }
        Member member = memberServiceImpl.findById(id);
        if (member == null) {
            return MgrResponseDto.error(MgrResponseCode.MEMBER_NOT_FOUND);
        }
        return MgrResponseDto.success(member);
    }

    @RequestMapping(value = "/user/getAll", method = RequestMethod.GET)
    public MgrResponseDto<List<Member>> getAllMembers() {
        // If not the coordinator server, route to the coordinator server
        if (SERVER_ID != 0) {
            log.info("Forwarding /user/getAll request to coordinator (server 0)");
            return serverRouter.forwardGetRaw(
                    0,
                    "/user/getAll",
                    new ParameterizedTypeReference<MgrResponseDto<List<Member>>>() {}
            );
        }
        List<Member> all = new ArrayList<>(memberServiceImpl.findAllMembers());
        for (int i = 1; i < 3; i++) {
            List<Member> remote = serverRouter.forwardGetRaw(
                    i, "/user/getAllLocal", new ParameterizedTypeReference<List<Member>>() {});
            all.addAll(remote);
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
        if (!isLocal(member.getId())) {
            // forward the request to the correct server
            return serverRouter.forwardPost(member.getId(), "/user/update", member, MgrResponseDto.class);
        }
        Member updatedMember = memberServiceImpl.updateMember(member);
        if (updatedMember == null) {
            return MgrResponseDto.error(MgrResponseCode.MEMBER_NAME_INVALID);
        }
        return MgrResponseDto.success(updatedMember);
    }

    @RequestMapping(value = "/user/delete/{id}", method = RequestMethod.DELETE)
    public MgrResponseDto<String> deleteMember(@PathVariable String id) {
        if (!isLocal(id)) {
            // forward the request to the correct server
            serverRouter.forwardDelete(id, "/user/delete/" + id);
            return MgrResponseDto.success("User deleted successfully");
        }
        memberServiceImpl.deleteMember(id);
        return MgrResponseDto.success("User deleted successfully");
    }

    private boolean isLocal(String id) {
        int toGoServerId = serverRouter.getServerIndex(id);
        log.info("Routing to server id: {}", toGoServerId);
        return toGoServerId == getCurrentServerIndex();
    }

    private int getCurrentServerIndex() {
        log.info("Local server id: {}", SERVER_ID);
        return SERVER_ID;
    }
}
