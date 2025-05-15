package org.distributed.shardingjh;

import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.controller.usercontroller.UserController;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.service.MemberService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

@SpringBootTest
class ShardingJhApplicationTests {

    @Autowired
    private UserController userController;

    @MockitoBean
    private MemberService memberService;

    @Test
    void contextLoads() {
    }

    @Test
    void testSaveUser() {
        Member member = new Member();
        member.setName("testUser");

        MgrResponseDto<Member> result = userController.save(member);

        assertEquals("0000", result.getCode());
        verify(memberService).saveUser(member);
    }
}
