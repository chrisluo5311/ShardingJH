package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.service.MemberService;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
public class UserController {

    @Resource
    MemberService memberService;

    @RequestMapping(value = "/user/save", method = RequestMethod.POST)
    public MgrResponseDto<Member> save(@RequestBody Member member) {
        Member newMember = memberService.saveUser(member);
        return MgrResponseDto.success(newMember);
    }

    @RequestMapping(value = "/user/get/{id}", method = RequestMethod.GET)
    public MgrResponseDto<Member> get(@PathVariable String id) {
        Member member = memberService.findById(id);
        return MgrResponseDto.success(member);
    }
}
