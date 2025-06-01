package org.distributed.shardingjh.heartbeat;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.distributed.shardingjh.gossip.GossipMsg;
import org.distributed.shardingjh.gossip.GossipService;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

/**
 * Heartbeat Sender - Sends heartbeat to all nodes in global finger table
 * Periodically checks the health status of all nodes to ensure network connectivity
 */
@Slf4j
@Component
public class hearBeatSender {

    @Resource
    private FingerTable fingerTable;

    @Resource
    private GossipService gossipService;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Value("${heartbeat.timeout:5000}")
    private int heartbeatTimeoutMs;

    @Value("${heartbeat.connect-timeout:3000}")
    private int connectTimeoutMs;

    private RestTemplate restTemplate;

    /**
     * Initialize RestTemplate with timeout configuration
     */
    @PostConstruct
    public void initRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(heartbeatTimeoutMs);
        
        this.restTemplate = new RestTemplateBuilder()
                .requestFactory(() -> factory)
                .build();
        
        log.info("[HeartBeat] RestTemplate initialized with connect timeout: {}ms, read timeout: {}ms", 
                connectTimeoutMs, heartbeatTimeoutMs);
    }

    /**
     * Scheduled heartbeat sending to all finger table nodes
     * Executes every 30 seconds
     */
    @Scheduled(fixedRate = 30000) // 30 seconds
    public void sendHeartbeatToAllNodes() {
        if (fingerTable.finger.isEmpty()) {
            log.warn("[HeartBeat] Finger table is empty, skipping heartbeat sending");
            return;
        }

        log.info("[HeartBeat] Starting to send heartbeat to all nodes, finger table size: {}", fingerTable.finger.size());
        
        fingerTable.finger.values().forEach(nodeUrl -> {
            if (!nodeUrl.equals(CURRENT_NODE_URL)) {
                sendHeartbeatAsync(nodeUrl);
            }
        });
    }

    /**
     * Asynchronously send heartbeat to specified node
     * @param targetNodeUrl Target node URL
     */
    @Async
    public CompletableFuture<Void> sendHeartbeatAsync(String targetNodeUrl) {
        try {
            sendHeartbeat(targetNodeUrl);
        } catch (Exception e) {
            log.error("[HeartBeat] Failed to send heartbeat to node {}: {}", targetNodeUrl, e.getMessage());
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Send heartbeat request to specified node
     * @param targetNodeUrl Target node URL
     */
    private void sendHeartbeat(String targetNodeUrl) {
        String heartbeatEndpoint = "/heartbeat/ping";
        String fullUrl = targetNodeUrl + heartbeatEndpoint;
        
        try {
            // Create heartbeat data
            HeartbeatData heartbeatData = HeartbeatData.builder()
                    .fromNode(CURRENT_NODE_URL)
                    .timestamp(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                    .message("ping")
                    .build();

            // Set request headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("X-Heartbeat-From", CURRENT_NODE_URL);

            HttpEntity<HeartbeatData> request = new HttpEntity<>(heartbeatData, headers);

            // Send heartbeat request with short timeout
            ResponseEntity<String> response = restTemplate.exchange(
                    fullUrl,
                    HttpMethod.POST,
                    request,
                    String.class
            );

            if (response.getStatusCode() == HttpStatus.OK) {
                log.debug("[HeartBeat] Successfully sent heartbeat to node {}", targetNodeUrl);
            } else {
                log.warn("[HeartBeat] Node {} heartbeat response abnormal, status code: {}", targetNodeUrl, response.getStatusCode());
                handleNodeFailure(targetNodeUrl);
            }

        } catch (Exception e) {
            log.error("[HeartBeat] Failed to send heartbeat to node {}: {}", targetNodeUrl, e.getMessage());
            handleNodeFailure(targetNodeUrl);
        }
    }

    /**
     * Handle node failure - Send gossip message and remove from finger table
     * @param failedNodeUrl Failed node URL
     */
    private void handleNodeFailure(String failedNodeUrl) {
        log.warn("[HeartBeat] Detected node failure: {}", failedNodeUrl);
        
        // Find the hash key for the failed node
        Integer failedNodeHash = findHashByNodeUrl(failedNodeUrl);
        if (failedNodeHash != null) {
            // Remove from local finger table
            fingerTable.finger.remove(failedNodeHash);
            log.info("[HeartBeat] Removed failed node from finger table: {} -> {}", failedNodeHash, failedNodeUrl);
            
            // Send HOST_DOWN gossip message
            sendHostDownGossip(failedNodeHash);
            
            log.info("[HeartBeat] Node failure handling completed for: {}", failedNodeUrl);
        } else {
            log.warn("[HeartBeat] Could not find hash for failed node: {}", failedNodeUrl);
        }
    }

    /**
     * Find hash key by node URL in finger table
     * @param nodeUrl Node URL to find
     * @return Hash key or null if not found
     */
    private Integer findHashByNodeUrl(String nodeUrl) {
        for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
            if (entry.getValue().equals(nodeUrl)) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * Send HOST_DOWN gossip message to notify other nodes
     * @param failedNodeHash Hash of the failed node
     */
    private void sendHostDownGossip(Integer failedNodeHash) {
        try {
            // Create HOST_DOWN gossip message
            GossipMsg gossipMsg = GossipMsg.builder()
                    .msgType(GossipMsg.Type.HOST_DOWN)
                    .msgContent(String.valueOf(failedNodeHash))
                    .build();
            
            // Send to random neighbors
            gossipService.randomSendGossip(gossipMsg, new ArrayList<>(fingerTable.finger.values()));
            
            log.info("[HeartBeat] Sent HOST_DOWN gossip message for node hash: {}", failedNodeHash);
        } catch (Exception e) {
            log.error("[HeartBeat] Failed to send HOST_DOWN gossip message: {}", e.getMessage());
        }
    }

    /**
     * Immediately send heartbeat to all nodes once (manual trigger)
     */
    public void sendImmediateHeartbeat() {
        log.info("[HeartBeat] Manual heartbeat trigger");
        sendHeartbeatToAllNodes();
    }

    /**
     * Get the current number of nodes in finger table
     * @return Number of nodes
     */
    public int getActiveNodesCount() {
        return fingerTable.finger.size();
    }

    /**
     * Heartbeat data entity class
     */
    public static class HeartbeatData {
        private String fromNode;
        private String timestamp;
        private String message;

        public static HeartbeatDataBuilder builder() {
            return new HeartbeatDataBuilder();
        }

        public static class HeartbeatDataBuilder {
            private String fromNode;
            private String timestamp;
            private String message;

            public HeartbeatDataBuilder fromNode(String fromNode) {
                this.fromNode = fromNode;
                return this;
            }

            public HeartbeatDataBuilder timestamp(String timestamp) {
                this.timestamp = timestamp;
                return this;
            }

            public HeartbeatDataBuilder message(String message) {
                this.message = message;
                return this;
            }

            public HeartbeatData build() {
                HeartbeatData data = new HeartbeatData();
                data.fromNode = this.fromNode;
                data.timestamp = this.timestamp;
                data.message = this.message;
                return data;
            }
        }

        // Getters and Setters
        public String getFromNode() { return fromNode; }
        public void setFromNode(String fromNode) { this.fromNode = fromNode; }
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
    }
}
