package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.config.ServerRouter;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.p2p.FingerTable;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.Impl.OrderServiceImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
public class OrderController {

    @Resource
    OrderServiceImpl orderServiceImpl;

    @Resource
    ServerRouter serverRouter;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Resource
    FingerTable fingerTable;

    @RequestMapping(value = "/order/save", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> saveOrder(@RequestBody RequestOrder requestOrder) {
        requestOrder.generateOrderId();
        String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(requestOrder.getOrderId());
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardPost(responsibleUrl, "/order/save", requestOrder, MgrResponseDto.class);
        }
        OrderTable order = orderServiceImpl.saveOrder(requestOrder);
        return MgrResponseDto.success(order);
    }

    @RequestMapping(value = "/order/delete", method = RequestMethod.POST)
    public MgrResponseDto<String> deleteOrder(@RequestBody OrderTable order) {
        String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(order.getId().getOrderId());
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardPost(responsibleUrl, "/order/delete", order, MgrResponseDto.class);
        }
        orderServiceImpl.deleteOrder(order);
        return MgrResponseDto.success("Order deleted successfully");
    }

    @RequestMapping(value = "/order/update", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> updateOrder(@RequestBody OrderTable order) {
        try {
            String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(order.getId().getOrderId());
            if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
                // forward the request to the correct server
                return serverRouter.forwardPost(responsibleUrl, "/order/update", order, MgrResponseDto.class);
            }
            OrderTable result = orderServiceImpl.updateOrder(order);
            return (result == null) ? MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND) : MgrResponseDto.success(result);
        } catch (IllegalStateException e) {
            return MgrResponseDto.error(MgrResponseCode.DB_CONFLICT);
        }
    }

    @RequestMapping(value = "/order/findRange", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderBetween(String startDate, String endDate) {
        List<OrderTable> all = new ArrayList<>(orderServiceImpl.findByCreateTimeBetween(startDate, endDate));

        for (String node : fingerTable.finger.values()) {
            if (!node.equals(CURRENT_NODE_URL)) {
                List<OrderTable> remote = serverRouter.forwardGetRaw(
                        node, "/order/findRangeLocal?startDate="+startDate+"&endDate="+endDate, new ParameterizedTypeReference<List<OrderTable>>() {});
                all.addAll(remote);
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
        String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(orderId);
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardGet(responsibleUrl, "/order/getOne?orderId=" + orderId + "&createTime=" + createTime, MgrResponseDto.class);
        }
        OrderTable result = orderServiceImpl.findByIdAndCreateTime(orderId, createTime);
        if (result == null) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(result);
    }

    @RequestMapping(value = "/order/history", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderHistory(String orderId, String createTime) {
        String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(orderId);
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardGet(responsibleUrl, "/order/history?orderId=" + orderId + "&createTime=" + createTime, MgrResponseDto.class);
        }
        List<OrderTable> history = orderServiceImpl.findAllVersions(orderId, createTime);
        if (history.isEmpty()) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(history);
    }
}
