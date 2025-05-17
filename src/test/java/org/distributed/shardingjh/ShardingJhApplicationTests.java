package org.distributed.shardingjh;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.config.ShardingProperties;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.controller.usercontroller.OrderController;
import org.distributed.shardingjh.controller.usercontroller.MemberController;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.OrderRepository;
import org.distributed.shardingjh.repository.user.MemberRepository;
import org.distributed.shardingjh.service.Impl.MemberServiceImpl;
import org.distributed.shardingjh.service.Impl.OrderServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

@Slf4j
@SpringBootTest
class ShardingJhApplicationTests {

    @Resource
    private MemberController memberController;

    @Resource
    private OrderController orderController;

    @MockitoBean
    private MemberServiceImpl memberServiceImpl;

    @MockitoBean
    private OrderServiceImpl orderServiceImpl;

    @Resource
    private MemberRepository memberRepository;

    @Resource
    private OrderRepository orderRepository;

    @Resource
    private ShardingProperties  shardingProperties;

    @Test
    void contextLoads() {
    }

    @Test
    void testSaveUser() {
        Member member = new Member();
        member.setName("testUser");

        MgrResponseDto<Member> result = memberController.saveMember(member);

        assertEquals(MgrResponseCode.SUCCESS.getCode(), result.getCode());
        verify(memberServiceImpl).saveMember(member);
    }

    @Test
    void testFindUser() {
        String randomUserId = UUID.randomUUID().toString();
        // random a member => expect error
        MgrResponseDto<Member> result = memberController.getOneMember(randomUserId);

        log.info("Get all members: {}", result);
        assertEquals(MgrResponseCode.MEMBER_NOT_FOUND.getCode(), result.getCode());
        verify(memberServiceImpl).findById(randomUserId);
    }

    @Test
    void findAllMember(){
        MgrResponseDto<List<Member>> result = memberController.getAllMembers();
        log.info("Get all members: {}", result);

        assertEquals(MgrResponseCode.SUCCESS.getCode(), result.getCode());
        verify(memberServiceImpl).findAllMembers();
    }

    @Test
    void findAllByCreatedTime() {
        LocalDateTime startDate = LocalDate.of(2024,6,1).atStartOfDay();
        ShardContext.setCurrentShard("shard_order_2024");
        List<OrderTable> result = orderRepository.findAllByCreateTimeAfter(startDate);
        log.info("After startDate: {}, result: {}", startDate,result);
        ShardContext.clear();

        LocalDateTime startDate2 = LocalDate.of(2025,1,1).atStartOfDay();
        LocalDateTime endDate = LocalDate.of(2025,8,1).atStartOfDay();
        ShardContext.setCurrentShard("shard_order_2025");
        List<OrderTable> orders = orderRepository.findByCreateTimeBetween(startDate2, endDate);
        log.info("Before endDate: {}, result: {}", endDate, orders);
        ShardContext.clear();
    }

    @Test
    void findOrdersBetween(){
        // same shard
        String startDate = LocalDate.of(2024,6,1).toString();
        String endDate = LocalDate.of(2024,6,30).toString();
        ShardContext.setCurrentShard("shard_order_2024");
        MgrResponseDto<List<OrderTable>> result = orderController.findOrderBetween(startDate, endDate);
        log.info("Get all orders from same shard: {}", result);
        assertEquals(MgrResponseCode.SUCCESS.getCode(), result.getCode());
        verify(orderServiceImpl).findByCreateTimeBetween(startDate, endDate);
        ShardContext.clear();

        // different shard
        String startDate2 = LocalDate.of(2024,6,1).toString();
        String endDate2 = LocalDate.of(2025,6,30).toString();
        ShardContext.setCurrentShard("shard_order_2025");
        MgrResponseDto<List<OrderTable>> result2 = orderController.findOrderBetween(startDate2, endDate2);
        log.info("Get all orders from different shard: {}", result2);
        assertEquals(MgrResponseCode.SUCCESS.getCode(), result2.getCode());
        verify(orderServiceImpl).findByCreateTimeBetween(startDate, endDate);
        ShardContext.clear();
    }
}
