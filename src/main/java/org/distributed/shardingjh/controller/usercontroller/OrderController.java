package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
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

    @RequestMapping(value = "/order/delete", method = RequestMethod.DELETE)
    public MgrResponseDto<String> deleteOrder(@RequestBody OrderTable order) {
        orderServiceImpl.deleteOrder(order);
        return MgrResponseDto.success("Order deleted successfully");
    }

    @RequestMapping(value = "/order/update", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> updateOrder(@RequestBody OrderTable order) {
        try {
            OrderTable result = orderServiceImpl.updateOrder(order);
            if (result == null) {
                return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
            }
            return MgrResponseDto.success(result);
        } catch (IllegalStateException e) {
            return MgrResponseDto.error(MgrResponseCode.DB_CONFLICT);
        }
    }

    @RequestMapping(value = "/order/findRange", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderBetween(String startDate, String endDate) {
        List<OrderTable> result = orderServiceImpl.findByCreateTimeBetween(startDate, endDate);
        if (result == null) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(result);
    }

    @RequestMapping(value = "/order/getOne", method = RequestMethod.GET)
    public MgrResponseDto<OrderTable> findOrderById(String orderId,String createTime) {
        OrderTable result = orderServiceImpl.findByIdAndCreateTime(orderId, createTime);
        if (result == null) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(result);
    }

    @RequestMapping(value = "/order/history", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderHistory(String orderId, String createTime) {
        List<OrderTable> history = orderServiceImpl.findAllVersions(orderId, createTime);
        if (history.isEmpty()) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(history);
    }
}
