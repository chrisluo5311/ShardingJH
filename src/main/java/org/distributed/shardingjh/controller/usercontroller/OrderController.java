package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.config.ServerRouter;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.Impl.OrderServiceImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@RestController
public class OrderController {

    @Resource
    OrderServiceImpl orderServiceImpl;

    @Resource
    ServerRouter serverRouter;

    @Value("${router.server-id}")
    private int SERVER_ID;

    @RequestMapping(value = "/order/save", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> saveOrder(@RequestBody RequestOrder requestOrder) {
        int month = requestOrder.getCreateTime().getMonthValue();
        if (!isLocal(month)) {
            // forward the request to the correct server
            return serverRouter.forwardOrderPost(month, "/order/save", requestOrder, MgrResponseDto.class);
        }
        OrderTable order = orderServiceImpl.saveOrder(requestOrder);
        return MgrResponseDto.success(order);
    }

    @RequestMapping(value = "/order/delete", method = RequestMethod.POST)
    public MgrResponseDto<String> deleteOrder(@RequestBody OrderTable order) {
        int month = order.getCreateTime().getMonthValue();
        if (!isLocal(month)) {
            // forward the request to the correct server
            return serverRouter.forwardOrderPost(month, "/order/delete", order, MgrResponseDto.class);
        }
        orderServiceImpl.deleteOrder(order);
        return MgrResponseDto.success("Order deleted successfully");
    }

    @RequestMapping(value = "/order/update", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> updateOrder(@RequestBody OrderTable order) {
        try {
            int month = order.getCreateTime().getMonthValue();
            if (!isLocal(month)) {
                // forward the request to the correct server
                return serverRouter.forwardOrderPost(month, "/order/update", order, MgrResponseDto.class);
            }
            OrderTable result = orderServiceImpl.updateOrder(order);
            return (result == null) ? MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND) : MgrResponseDto.success(result);
        } catch (IllegalStateException e) {
            return MgrResponseDto.error(MgrResponseCode.DB_CONFLICT);
        }
    }

    @RequestMapping(value = "/order/findRange", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderBetween(String startDate, String endDate) {
        Set<Integer> targetServers = new HashSet<>();

        LocalDate start = LocalDate.parse(startDate);
        LocalDate end = LocalDate.parse(endDate);
        LocalDate current = start;

        // Collect which servers are involved based on months in the range
        while (!current.isAfter(end)) {
            int serverIndex = serverRouter.getOrderServerIndex(current.getMonthValue());
            targetServers.add(serverIndex);
            current = current.plusMonths(1);
        }

        List<OrderTable> all = new ArrayList<>();

        for (Integer index : targetServers) {
            try {
                List<OrderTable> partial = serverRouter.forwardGetRaw(
                        index,
                        "/order/findRangeLocal?startDate=" + startDate + "&endDate=" + endDate,
                        new ParameterizedTypeReference<List<OrderTable>>() {}
                );
                all.addAll(partial);
            } catch (Exception e) {
                log.error("Failed to fetch from server {}: {}", index, e.getMessage());
                return MgrResponseDto.error(MgrResponseCode.SERVER_ERROR);
            }
        }

        if (all.isEmpty()) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }

        return MgrResponseDto.success(all);
    }

    @GetMapping("/order/findRangeLocal")
    public List<OrderTable> findLocalOrderBetween(@RequestParam String startDate, @RequestParam String endDate) {
        return orderServiceImpl.findByCreateTimeBetween(startDate, endDate);
    }

    @RequestMapping(value = "/order/getOne", method = RequestMethod.GET)
    public MgrResponseDto<OrderTable> findOrderById(String orderId,String createTime) {
        int month = LocalDate.parse(createTime).getMonthValue();
        if (!isLocal(month)) {
            // forward the request to the correct server
            return serverRouter.forwardOrderGet(month, "/order/getOne?orderId=" + orderId + "&createTime=" + createTime, MgrResponseDto.class);
        }
        OrderTable result = orderServiceImpl.findByIdAndCreateTime(orderId, createTime);
        if (result == null) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(result);
    }

    @RequestMapping(value = "/order/history", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderHistory(String orderId, String createTime) {
        int month = LocalDate.parse(createTime).getMonthValue();
        if (!isLocal(month)) {
            // forward the request to the correct server
            return serverRouter.forwardOrderGet(month, "/order/history?orderId=" + orderId + "&createTime=" + createTime, MgrResponseDto.class);
        }
        List<OrderTable> history = orderServiceImpl.findAllVersions(orderId, createTime);
        if (history.isEmpty()) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(history);
    }

    private boolean isLocal(int month) {
        int toGoServerId = serverRouter.getOrderServerIndex(month);
        log.info("[OrderController] Routing to server id: {}", toGoServerId);
        return toGoServerId == getCurrentServerIndex();
    }

    private int getCurrentServerIndex() {
        log.info("[OrderController] Local server id: {}", SERVER_ID);
        return SERVER_ID;
    }
}
