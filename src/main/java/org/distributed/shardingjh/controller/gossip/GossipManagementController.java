package org.distributed.shardingjh.controller.gossip;

import java.util.HashMap;
import java.util.Map;

import org.distributed.shardingjh.gossip.GossipService;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/gossip")
public class GossipManagementController {

    @Resource
    private GossipService gossipService;

    @Resource
    private FingerTable fingerTable;

    /**
     * Get current finger table status
     */
    @GetMapping("/finger-table")
    public ResponseEntity<Map<String, Object>> getFingerTable() {
        try {
            Map<String, Object> response = new HashMap<>();
            response.put("fingerTable", fingerTable.finger);
            response.put("size", fingerTable.finger.size());
            response.put("timestamp", System.currentTimeMillis());
            
            log.info("[GossipManagement] Finger table requested via API: {}", fingerTable.finger);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("[GossipManagement] Error getting finger table: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Clean up duplicate nodes in finger table
     */
    @PostMapping("/cleanup-duplicates")
    public ResponseEntity<Map<String, Object>> cleanupDuplicates() {
        try {
            Map<Integer, String> beforeCleanup = new HashMap<>(fingerTable.finger);
            int sizeBefore = fingerTable.finger.size();
            
            gossipService.cleanupDuplicateNodes();
            
            int sizeAfter = fingerTable.finger.size();
            int removedCount = sizeBefore - sizeAfter;
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("beforeCleanup", beforeCleanup);
            response.put("afterCleanup", fingerTable.finger);
            response.put("sizeBefore", sizeBefore);
            response.put("sizeAfter", sizeAfter);
            response.put("removedCount", removedCount);
            response.put("timestamp", System.currentTimeMillis());
            
            log.info("[GossipManagement] Duplicate cleanup completed. Removed {} entries", removedCount);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("[GossipManagement] Error during cleanup: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Force broadcast current finger table state
     */
    @PostMapping("/force-broadcast")
    public ResponseEntity<Map<String, Object>> forceBroadcast() {
        try {
            // This will trigger the next scheduled broadcast
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Force broadcast requested - will occur on next scheduled interval");
            response.put("currentFingerTable", fingerTable.finger);
            response.put("timestamp", System.currentTimeMillis());
            
            log.info("[GossipManagement] Force broadcast requested");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("[GossipManagement] Error forcing broadcast: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Check for duplicate nodes without removing them
     */
    @GetMapping("/check-duplicates")
    public ResponseEntity<Map<String, Object>> checkDuplicates() {
        try {
            Map<String, Integer> addressCount = new HashMap<>();
            Map<String, java.util.List<Integer>> duplicates = new HashMap<>();
            
            // Count occurrences of each address
            for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
                String address = entry.getValue();
                addressCount.put(address, addressCount.getOrDefault(address, 0) + 1);
            }
            
            // Find duplicates
            for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
                String address = entry.getValue();
                if (addressCount.get(address) > 1) {
                    duplicates.computeIfAbsent(address, k -> new java.util.ArrayList<>()).add(entry.getKey());
                }
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("hasDuplicates", !duplicates.isEmpty());
            response.put("duplicates", duplicates);
            response.put("totalDuplicateNodes", duplicates.size());
            response.put("fingerTable", fingerTable.finger);
            response.put("timestamp", System.currentTimeMillis());
            
            log.info("[GossipManagement] Duplicate check completed. Found {} duplicate nodes", duplicates.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("[GossipManagement] Error checking duplicates: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
} 