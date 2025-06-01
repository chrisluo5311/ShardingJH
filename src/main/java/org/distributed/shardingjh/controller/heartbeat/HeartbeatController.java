package org.distributed.shardingjh.controller.heartbeat;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.heartbeat.hearBeatSender;
import org.distributed.shardingjh.heartbeat.heartBeatReceiver;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

/**
 * Heartbeat Controller - Provides heartbeat related management and monitoring APIs
 * Used for manual heartbeat triggering, viewing heartbeat status, etc.
 */
@Slf4j
@RestController
@RequestMapping("/api/heartbeat")
public class HeartbeatController {

    @Resource
    private hearBeatSender heartBeatSender;

    @Resource
    private heartBeatReceiver heartBeatReceiver;

    @Resource
    private FingerTable fingerTable;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    /**
     * Manually trigger heartbeat sending to all nodes
     * @return Trigger result
     */
    @PostMapping("/trigger")
    public ResponseEntity<MgrResponseDto<String>> triggerHeartbeat() {
        try {
            log.info("[HeartbeatController] Manual heartbeat trigger");
            heartBeatSender.sendImmediateHeartbeat();
            
            return ResponseEntity.ok(MgrResponseDto.success("Heartbeat sending triggered"));
        } catch (Exception e) {
            log.error("[HeartbeatController] Failed to trigger heartbeat: {}", e.getMessage());
            return ResponseEntity.ok(MgrResponseDto.error("9999", "Failed to trigger heartbeat: " + e.getMessage()));
        }
    }

    /**
     * Get heartbeat system status
     * @return Heartbeat system status information
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getHeartbeatSystemStatus() {
        Map<String, Object> status = new HashMap<>();
        
        try {
            status.put("currentNode", CURRENT_NODE_URL);
            status.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            status.put("fingerTableSize", fingerTable.finger.size());
            status.put("activeNodesCount", heartBeatSender.getActiveNodesCount());
            status.put("heartbeatRecordsCount", heartBeatReceiver.getHeartbeatRecordsCount());
            status.put("fingerTableNodes", fingerTable.finger);
            status.put("status", "healthy");
            
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            log.error("[HeartbeatController] Failed to get heartbeat status: {}", e.getMessage());
            status.put("status", "error");
            status.put("error", e.getMessage());
            return ResponseEntity.ok(status);
        }
    }

    /**
     * Get heartbeat information for specified node
     * @param nodeUrl Node URL (needs URL encoding)
     * @return Node heartbeat information
     */
    @GetMapping("/node/{nodeUrl}")
    public ResponseEntity<Map<String, Object>> getNodeHeartbeatInfo(@PathVariable String nodeUrl) {
        // Decode URL parameter (replace _ with /, replace - with :)
        String decodedNodeUrl = nodeUrl.replace("_", "/").replace("-", ":");
        
        Map<String, Object> nodeInfo = new HashMap<>();
        nodeInfo.put("nodeUrl", decodedNodeUrl);
        nodeInfo.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        nodeInfo.put("hasHeartbeatRecord", heartBeatReceiver.hasHeartbeatRecord(decodedNodeUrl));
        nodeInfo.put("lastHeartbeatTime", heartBeatReceiver.getLastHeartbeatTime(decodedNodeUrl));
        nodeInfo.put("isInFingerTable", fingerTable.finger.containsValue(decodedNodeUrl));
        
        return ResponseEntity.ok(nodeInfo);
    }

    /**
     * Get finger table information
     * @return Finger table detailed information
     */
    @GetMapping("/finger-table")
    public ResponseEntity<Map<String, Object>> getFingerTableInfo() {
        Map<String, Object> fingerTableInfo = new HashMap<>();
        fingerTableInfo.put("currentNode", CURRENT_NODE_URL);
        fingerTableInfo.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        fingerTableInfo.put("fingerTable", fingerTable.finger);
        fingerTableInfo.put("fingerTableSize", fingerTable.finger.size());
        fingerTableInfo.put("fingerTableString", fingerTable.toString());
        
        return ResponseEntity.ok(fingerTableInfo);
    }

    /**
     * Heartbeat system health check
     * @return Health status
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        
        try {
            boolean isHealthy = !fingerTable.finger.isEmpty();
            
            health.put("status", isHealthy ? "healthy" : "unhealthy");
            health.put("node", CURRENT_NODE_URL);
            health.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            health.put("fingerTableSize", fingerTable.finger.size());
            health.put("details", isHealthy ? "Heartbeat system running normally" : "Finger table is empty");
            
            return ResponseEntity.ok(health);
        } catch (Exception e) {
            health.put("status", "error");
            health.put("error", e.getMessage());
            return ResponseEntity.ok(health);
        }
    }

    /**
     * Get heartbeat configuration information
     * @return Heartbeat configuration
     */
    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getHeartbeatConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("currentNode", CURRENT_NODE_URL);
        config.put("heartbeatInterval", "30 seconds");
        config.put("heartbeatEndpoint", "/heartbeat/ping");
        config.put("heartbeatTimeout", "System default");
        config.put("asyncEnabled", true);
        config.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        
        return ResponseEntity.ok(config);
    }
} 