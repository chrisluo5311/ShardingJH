package org.distributed.shardingjh.controller.usercontroller;

import io.lettuce.core.dynamic.annotation.Param;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.weaver.ast.Or;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.Impl.OrderServiceImpl;
import org.springframework.web.bind.annotation.*;

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

    @RequestMapping(value = "/order/get", method = RequestMethod.GET)
    public MgrResponseDto<OrderTable> findOrderById(String orderId,String createTime) {
        OrderTable result = orderServiceImpl.findByIdAndCreateTime(orderId, createTime);
        if (result == null) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(result);
    }

    @RequestMapping(value = "/order/update", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> updateOrder(@RequestBody OrderTable order) {
        OrderTable result = orderServiceImpl.updateOrder(order);
        if (result == null) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(result);
    }

    @RequestMapping(value = "/order/delete", method = RequestMethod.DELETE)
    public MgrResponseDto<String> deleteOrder(@RequestBody OrderTable order) {
        orderServiceImpl.deleteOrder(order);
        return MgrResponseDto.success("Order deleted successfully");
    }
}
