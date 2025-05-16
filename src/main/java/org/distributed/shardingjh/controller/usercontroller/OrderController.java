package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.Impl.OrderServiceImpl;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
public class OrderController {

    @Resource
    OrderServiceImpl orderServiceImpl;

    @RequestMapping(value = "/order/save", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> saveOrder(@RequestBody RequestOrder requestOrder) {
        OrderTable order = orderServiceImpl.saveOrder(requestOrder);
        return MgrResponseDto.success(order);
    }


    @RequestMapping(value = "/order/find", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderBetween(String startDate, String endDate) {
        List<OrderTable> result = orderServiceImpl.findByCreateTimeBetween(startDate, endDate);
        if (result == null) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(result);
    }
}
