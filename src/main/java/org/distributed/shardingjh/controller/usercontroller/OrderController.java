package org.distributed.shardingjh.controller.usercontroller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.response.MgrResponseCode;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.config.ServerRouter;
import org.distributed.shardingjh.model.OrderTable;
import org.distributed.shardingjh.p2p.FingerTable;
import org.distributed.shardingjh.repository.order.RequestOrder;
import org.distributed.shardingjh.service.Impl.OrderServiceImpl;
import org.distributed.shardingjh.util.EncryptUtil;
import org.distributed.shardingjh.util.OrderSignatureUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
public class OrderController {

    @Resource
    OrderServiceImpl orderServiceImpl;

    @Resource
    ServerRouter serverRouter;

    @Resource
    private ObjectMapper objectMapper;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Value("${sha256.secret.key}")
    private String SECRET_KEY;

    @Resource
    FingerTable fingerTable;

    @RequestMapping(value = "/order/save", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> saveOrder(@RequestBody RequestOrder requestOrder,
                                                @RequestHeader(value = "X-Signature") String signature) throws JsonProcessingException {
        // Check signature
        String rawBody = OrderSignatureUtil.toCanonicalJson(requestOrder, objectMapper);
        String expectedSignature = EncryptUtil.hmacSha256(rawBody, SECRET_KEY);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for order: {}", requestOrder.getOrderId());
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }

        // Determine responsible server
        String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(requestOrder.getOrderId());
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardPost(responsibleUrl, "/order/save", signature, requestOrder, MgrResponseDto.class);

        }
        OrderTable order = orderServiceImpl.saveOrder(requestOrder);
        return MgrResponseDto.success(order);
    }

    @RequestMapping(value = "/order/delete", method = RequestMethod.POST)
    public MgrResponseDto<String> deleteOrder(@RequestBody OrderTable order,
                                                @RequestHeader(value = "X-Signature") String signature) throws JsonProcessingException {
        // Check signature
        String rawBody = OrderSignatureUtil.toCanonicalJson(order, objectMapper);
        String expectedSignature = EncryptUtil.hmacSha256(rawBody, SECRET_KEY);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for order deletion: {}", order.getId().getOrderId());
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }

        // Determine responsible server
        String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(order.getId().getOrderId());
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardPost(responsibleUrl, "/order/delete", signature, order, MgrResponseDto.class);

        }
        orderServiceImpl.deleteOrder(order);
        return MgrResponseDto.success("Order deleted successfully");
    }

    @RequestMapping(value = "/order/update", method = RequestMethod.POST)
    public MgrResponseDto<OrderTable> updateOrder(@RequestBody OrderTable order,
                                                    @RequestHeader(value = "X-Signature") String signature) throws JsonProcessingException {
        try {
            // Check signature
            String rawBody = OrderSignatureUtil.toCanonicalJson(order, objectMapper);
            log.info("rawBody: {}", rawBody);
            String expectedSignature = EncryptUtil.hmacSha256(rawBody, SECRET_KEY);
            log.info("Expected signature: {}, X-Signature: {}", expectedSignature, signature);
            if (!expectedSignature.equals(signature)) {
                log.error("Signature mismatch for order update: {}", order.getId().getOrderId());
                return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
            }

            // Determine responsible server
            String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(order.getId().getOrderId());
            if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
                // forward the request to the correct server
                return serverRouter.forwardPost(responsibleUrl, "/order/update", signature, order, MgrResponseDto.class);
            }
            OrderTable result = orderServiceImpl.updateOrder(order);
            return (result == null) ? MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND) : MgrResponseDto.success(result);
        } catch (Exception e) {
            return MgrResponseDto.error(MgrResponseCode.DB_CONFLICT.getCode(),MgrResponseCode.DB_CONFLICT.getMessage()+
                    " - " + e.getMessage());
        }
    }

    @RequestMapping(value = "/order/findRange", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderBetween(String startDate, String endDate,
                                                                @RequestHeader(value = "X-Signature", required = false) String signature) {
        // Check signature
        String endPointPath = "/order/findRange?startDate=" + startDate + "&endDate=" + endDate;
        String expectedSignature = EncryptUtil.hmacSha256(endPointPath, SECRET_KEY);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for order range query: startDate={}, endDate={}", startDate, endDate);
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }

        List<OrderTable> all = new ArrayList<>(orderServiceImpl.findByCreateTimeBetween(startDate, endDate));

        for (String node : fingerTable.finger.values()) {
            if (!node.equals(CURRENT_NODE_URL)) {
                List<OrderTable> remote = serverRouter.forwardGetRaw(
                        node, "/order/findRangeLocal?startDate="+startDate+"&endDate="+endDate,
                        signature, new ParameterizedTypeReference<List<OrderTable>>() {});
                if (remote == null || remote.isEmpty()) {
                    log.warn("No orders found on node: {}", node);
                    continue;
                }
                all.addAll(remote);
            }
        }

        if (all.isEmpty()) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }

        return MgrResponseDto.success(all);
    }

    /**
     * Internal endpoint for testing
     * */
    @GetMapping("/order/findRangeLocal")
    public List<OrderTable> findLocalOrderBetween(@RequestParam String startDate, @RequestParam String endDate) {
        return orderServiceImpl.findByCreateTimeBetween(startDate, endDate);
    }

    @RequestMapping(value = "/order/getOne", method = RequestMethod.GET)
    public MgrResponseDto<OrderTable> findOrderById(String orderId,String createTime,
                                                    @RequestHeader(value = "X-Signature", required = false) String signature) {
        // Check signature
        String endPointPath = "/order/getOne?orderId=" + orderId + "&createTime=" + createTime;
        String expectedSignature = EncryptUtil.hmacSha256(endPointPath, SECRET_KEY);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for order query: orderId={}, createTime={}", orderId, createTime);
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }

        // Determine responsible server
        String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(orderId);
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardGet(responsibleUrl,
                    "/order/getOne?orderId=" + orderId + "&createTime=" + createTime,
                    signature, MgrResponseDto.class);
        }
        OrderTable result = orderServiceImpl.findByIdAndCreateTime(orderId, createTime);
        if (result == null) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(result);
    }

    @RequestMapping(value = "/order/history", method = RequestMethod.GET)
    public MgrResponseDto<List<OrderTable>> findOrderHistory(String orderId, String createTime,
                                                                @RequestHeader(value = "X-Signature", required = false) String signature) {
        // Check signature
        String endPointPath = "/order/history?orderId=" + orderId + "&createTime=" + createTime;
        String expectedSignature = EncryptUtil.hmacSha256(endPointPath, SECRET_KEY);
        log.info("expectedSignature: {}, signature: {}", expectedSignature, signature);
        if (!expectedSignature.equals(signature)) {
            log.error("Signature mismatch for order history query: orderId={}, createTime={}", orderId, createTime);
            return MgrResponseDto.error(MgrResponseCode.UNAUTHORIZED_REQUEST);
        }

        String responsibleUrl = serverRouter.getOrderResponsibleServerUrl(orderId);
        if (!CURRENT_NODE_URL.equals(responsibleUrl)) {
            // forward the request to the correct server
            return serverRouter.forwardGet(responsibleUrl,
                    "/order/history?orderId=" + orderId + "&createTime=" + createTime,
                    signature, MgrResponseDto.class);
        }
        List<OrderTable> history = orderServiceImpl.findAllVersions(orderId, createTime);
        if (history.isEmpty()) {
            return MgrResponseDto.error(MgrResponseCode.ORDER_NOT_FOUND);
        }
        return MgrResponseDto.success(history);
    }

    /**
     * Internal test endpoint to demonstrate retry and rollback mechanism.
     * */
    @PostMapping("/order/updateAndFail")
    public ResponseEntity<String> updateAndFail(@RequestBody OrderTable order) {
        orderServiceImpl.updateWithRetryAndRollback(order);
        return ResponseEntity.ok("Should never reach here");
    }
}
