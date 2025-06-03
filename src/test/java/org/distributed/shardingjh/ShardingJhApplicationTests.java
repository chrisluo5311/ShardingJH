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

//    @Resource
//    private MemberController memberController;

//
//    @MockitoBean
//    private MemberServiceImpl memberServiceImpl;

//
//    @Resource
//    private MemberRepository memberRepository;

//    @Resource
//    private ShardingProperties  shardingProperties;

}
