package org.distributed.shardingjh.heartbeat;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.distributed.shardingjh.gossip.GossipMsg;
import org.distributed.shardingjh.gossip.GossipService;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

/**
 * Heartbeat Receiver - Receives and processes heartbeat requests from other nodes
 * Maintains node activity status records
 */
@Slf4j
@RestController
@RequestMapping("/heartbeat")
public class heartBeatReceiver {

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Resource
    private FingerTable fingerTable;

    @Resource
    private GossipService gossipService;

    // Store last heartbeat time for each node
    private final ConcurrentHashMap<String, String> nodeLastHeartbeat = new ConcurrentHashMap<>();

    // Heartbeat record expiration time in minutes (default: 3 minutes)
    private static final long HEARTBEAT_EXPIRATION_MINUTES = 3;

    /**
     * Receive heartbeat ping requests
     * @param heartbeatData Heartbeat data
     * @param heartbeatFrom Sending node information from request header
     * @return Heartbeat response
     */
    @PostMapping("/ping")
    public ResponseEntity<Map<String, Object>> receiveHeartbeat(
            @RequestBody hearBeatSender.HeartbeatData heartbeatData,
            @RequestHeader(value = "X-Heartbeat-From", required = false) String heartbeatFrom) {
        
        try {
            String fromNode = heartbeatData.getFromNode();
            String timestamp = heartbeatData.getTimestamp();
            
            log.debug("[HeartBeat] Received heartbeat from node {}, timestamp: {}", fromNode, timestamp);
            
            // Update node last heartbeat time
            nodeLastHeartbeat.put(fromNode, LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            
            // Build response
            Map<String, Object> response = new HashMap<>();
            response.put("status", "ok");
            response.put("message", "pong");
            response.put("fromNode", CURRENT_NODE_URL);
            response.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            response.put("receivedFrom", fromNode);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("[HeartBeat] Failed to process heartbeat request: {}", e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Heartbeat processing failed");
            errorResponse.put("error", e.getMessage());
            
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Scheduled cleanup of expired heartbeat records
     * Runs every 3 minutes to clean up records older than 3 minutes
     */
    @Scheduled(fixedRate = 180000) // 3 minutes
    public void cleanupExpiredHeartbeats() {
        log.info("[HeartBeat] Starting cleanup - Current finger table: {}", fingerTable.finger);
        log.info("[HeartBeat] Current heartbeat records: {}", nodeLastHeartbeat);
        
        LocalDateTime now = LocalDateTime.now();
        int removedCount = 0;
        
        // Create a copy of the keys to avoid ConcurrentModificationException
        var nodeUrls = nodeLastHeartbeat.keySet().toArray(new String[0]);
        
        for (String nodeUrl : nodeUrls) {
            String lastHeartbeatStr = nodeLastHeartbeat.get(nodeUrl);
            if (lastHeartbeatStr != null) {
                try {
                    LocalDateTime lastHeartbeat = LocalDateTime.parse(lastHeartbeatStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    long minutesSinceLastHeartbeat = ChronoUnit.MINUTES.between(lastHeartbeat, now);
                    
                    if (minutesSinceLastHeartbeat > HEARTBEAT_EXPIRATION_MINUTES) {
                        nodeLastHeartbeat.remove(nodeUrl);
                        removedCount++;
                        
                        // Confirm node failure and trigger appropriate actions
                        confirmNodeFailure(nodeUrl, minutesSinceLastHeartbeat);
                        
                        log.info("[HeartBeat] CONFIRMED node failure: {} (last seen {} minutes ago)", 
                                nodeUrl, minutesSinceLastHeartbeat);
                    }
                } catch (Exception e) {
                    log.warn("[HeartBeat] Failed to parse heartbeat timestamp for node {}: {}", nodeUrl, e.getMessage());
                    // Remove invalid timestamp records
                    nodeLastHeartbeat.remove(nodeUrl);
                    removedCount++;
                }
            }
        }
        
        if (removedCount > 0) {
            log.info("[HeartBeat] Cleanup completed: removed {} expired heartbeat records, {} active records remaining", 
                    removedCount, nodeLastHeartbeat.size());
        }
    }

    /**
     * Confirm node failure and trigger appropriate actions
     * This is the authoritative failure detection point
     * @param nodeUrl Failed node URL
     * @param minutesOffline Minutes since last heartbeat
     */
    private void confirmNodeFailure(String nodeUrl, long minutesOffline) {
        log.warn("[HeartBeat] *** NODE FAILURE CONFIRMED *** Node: {}, Offline for: {} minutes", nodeUrl, minutesOffline);
        
        // Find the hash key for the failed node
        Integer failedNodeHash = findHashByNodeUrl(nodeUrl);
        if (failedNodeHash != null) {
            // Remove from local finger table
            fingerTable.finger.remove(failedNodeHash);
            log.info("[HeartBeat] Removed failed node from finger table: {} -> {}", failedNodeHash, nodeUrl);
            
            // Send HOST_DOWN gossip message
            sendHostDownGossip(failedNodeHash);
            
            log.info("[HeartBeat] Node failure confirmation completed for: {}", nodeUrl);
        } else {
            log.warn("[HeartBeat] Could not find hash for failed node: {}", nodeUrl);
        }
    }

    /**
     * Find hash key by node URL in finger table
     * @param nodeUrl Node URL to find
     * @return Hash key or null if not found
     */
    private Integer findHashByNodeUrl(String nodeUrl) {
        log.debug("[HeartBeat] Looking for hash of failed node: '{}' in finger table", nodeUrl);
        log.debug("[HeartBeat] Current finger table entries: {}", fingerTable.finger);
        
        for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
            log.debug("[HeartBeat] Comparing '{}' with finger table entry: '{}' -> '{}'", 
                    nodeUrl, entry.getKey(), entry.getValue());
            if (entry.getValue().equals(nodeUrl)) {
                log.info("[HeartBeat] Found matching hash: {} for node: {}", entry.getKey(), nodeUrl);
                return entry.getKey();
            }
        }
        log.warn("[HeartBeat] No matching hash found for node: '{}' in finger table with {} entries", 
                nodeUrl, fingerTable.finger.size());
        return null;
    }

    /**
     * Send HOST_DOWN gossip message to notify other nodes
     * @param failedNodeHash Hash of the failed node
     */
    private void sendHostDownGossip(Integer failedNodeHash) {
        try {
            // Create HOST_DOWN gossip message with unique identifier
            GossipMsg gossipMsg = GossipMsg.builder()
                    .msgType(GossipMsg.Type.HOST_DOWN)
                    .msgContent(String.valueOf(failedNodeHash))
                    .senderId(CURRENT_NODE_URL)
                    .timestamp(String.valueOf(System.currentTimeMillis()))
                    .build();
            
            // Send to random neighbors
            gossipService.randomSendGossip(gossipMsg, new ArrayList<>(fingerTable.finger.values()));
            
            log.info("[HeartBeat] Sent HOST_DOWN gossip message for node hash: {}", failedNodeHash);
        } catch (Exception e) {
            log.error("[HeartBeat] Failed to send HOST_DOWN gossip message: {}", e.getMessage());
        }
    }

    /**
     * Get last heartbeat time for all nodes
     * @return Node heartbeat status information
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getHeartbeatStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("currentNode", CURRENT_NODE_URL);
        status.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        status.put("nodeLastHeartbeat", new HashMap<>(nodeLastHeartbeat));
        status.put("activeNodesCount", nodeLastHeartbeat.size());
        status.put("heartbeatExpirationMinutes", HEARTBEAT_EXPIRATION_MINUTES);
        
        return ResponseEntity.ok(status);
    }

    /**
     * Health check endpoint
     * @return Current node health status
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "healthy");
        health.put("node", CURRENT_NODE_URL);
        health.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        
        return ResponseEntity.ok(health);
    }

    /**
     * Clear heartbeat record for specified node
     * @param nodeUrl Node URL
     * @return Operation result
     */
    @DeleteMapping("/clear/{nodeUrl}")
    public ResponseEntity<Map<String, Object>> clearNodeHeartbeat(@PathVariable String nodeUrl) {
        String decodedNodeUrl = nodeUrl.replace("_", "/").replace("-", ":");
        
        if (nodeLastHeartbeat.remove(decodedNodeUrl) != null) {
            log.info("[HeartBeat] Cleared heartbeat record for node {}", decodedNodeUrl);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Node heartbeat record cleared");
            response.put("clearedNode", decodedNodeUrl);
            
            return ResponseEntity.ok(response);
        } else {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "not_found");
            response.put("message", "Heartbeat record for specified node not found");
            response.put("nodeUrl", decodedNodeUrl);
            
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        }
    }

    /**
     * Manually trigger cleanup of expired heartbeat records
     * @return Cleanup result
     */
    @PostMapping("/cleanup")
    public ResponseEntity<Map<String, Object>> manualCleanup() {
        int sizeBefore = nodeLastHeartbeat.size();
        cleanupExpiredHeartbeats();
        int sizeAfter = nodeLastHeartbeat.size();
        int removedCount = sizeBefore - sizeAfter;
        
        Map<String, Object> result = new HashMap<>();
        result.put("status", "success");
        result.put("message", "Manual cleanup completed");
        result.put("removedRecords", removedCount);
        result.put("remainingRecords", sizeAfter);
        result.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        
        return ResponseEntity.ok(result);
    }

    /**
     * Get number of heartbeat records
     * @return Number of heartbeat records
     */
    public int getHeartbeatRecordsCount() {
        return nodeLastHeartbeat.size();
    }

    /**
     * Check if specified node has heartbeat record
     * @param nodeUrl Node URL
     * @return Whether heartbeat record exists
     */
    public boolean hasHeartbeatRecord(String nodeUrl) {
        return nodeLastHeartbeat.containsKey(nodeUrl);
    }

    /**
     * Get last heartbeat time for specified node
     * @param nodeUrl Node URL
     * @return Last heartbeat time, returns null if not exists
     */
    public String getLastHeartbeatTime(String nodeUrl) {
        return nodeLastHeartbeat.get(nodeUrl);
    }

    /**
     * Check if a node is considered active (heartbeat within expiration time)
     * @param nodeUrl Node URL
     * @return Whether the node is active
     */
    public boolean isNodeActive(String nodeUrl) {
        String lastHeartbeatStr = nodeLastHeartbeat.get(nodeUrl);
        if (lastHeartbeatStr == null) {
            return false;
        }
        
        try {
            LocalDateTime lastHeartbeat = LocalDateTime.parse(lastHeartbeatStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            long minutesSinceLastHeartbeat = ChronoUnit.MINUTES.between(lastHeartbeat, LocalDateTime.now());
            return minutesSinceLastHeartbeat <= HEARTBEAT_EXPIRATION_MINUTES;
        } catch (Exception e) {
            log.warn("[HeartBeat] Failed to check node activity for {}: {}", nodeUrl, e.getMessage());
            return false;
        }
    }
}
