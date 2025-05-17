package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.service.Impl.MemberServiceImpl;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
public class MemberController {

    @Resource
    MemberServiceImpl memberServiceImpl;

    @RequestMapping(value = "/user/save", method = RequestMethod.POST)
    public MgrResponseDto<Member> saveMember(@RequestBody Member member) {
        Member newMember = memberServiceImpl.saveMember(member);
        return MgrResponseDto.success(newMember);
    }

    @RequestMapping(value = "/user/get/{id}", method = RequestMethod.GET)
    public MgrResponseDto<Member> getOneMember(@PathVariable String id) {
        Member member = memberServiceImpl.findById(id);
        if (member == null) {
            return MgrResponseDto.error(MgrResponseCode.MEMBER_NOT_FOUND);
        }
        return MgrResponseDto.success(member);
    }

    @RequestMapping(value = "/user/getAll", method = RequestMethod.GET)
    public MgrResponseDto<List<Member>> getAllMembers() {
        return MgrResponseDto.success(memberServiceImpl.findAllMembers());
    }
}
