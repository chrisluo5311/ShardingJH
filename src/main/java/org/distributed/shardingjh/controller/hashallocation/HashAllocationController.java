package org.distributed.shardingjh.controller.hashallocation;

import java.util.HashMap;
import java.util.Map;

import org.distributed.shardingjh.gossip.DynamicHashAllocator;
import org.distributed.shardingjh.gossip.NodeJoinRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

/**
 * Hash Allocation Controller - Handles hash allocation requests via HTTP
 * This provides reliable communication for critical hash allocation operations
 */
@Slf4j
@RestController
@RequestMapping("/hash-allocation")
public class HashAllocationController {
    
    @Resource
    private DynamicHashAllocator dynamicHashAllocator;
    
    /**
     * Handle hash proposal requests
     * @param request Hash proposal request
     * @return Proposal response (accept/reject)
     */
    @PostMapping("/proposal")
    public ResponseEntity<Map<String, Object>> handleHashProposal(@RequestBody NodeJoinRequest request) {
        log.info("[HashAllocationController] 📥 Received hash proposal via HTTP: {} from: {}", 
                request.getProposedHash(), request.getNodeUrl());
        
        try {
            // Process the proposal using existing logic
            ProposalResult result = dynamicHashAllocator.processHashProposalHTTP(request);
            
            Map<String, Object> response = new HashMap<>();
            response.put("accepted", result.isAccepted());
            response.put("reason", result.getReason());
            response.put("respondingNode", result.getRespondingNode());
            response.put("proposedHash", request.getProposedHash());
            response.put("conflictResolver", request.generateConflictResolver());
            
            log.info("[HashAllocationController] 📤 Sending proposal response: {} ({})", 
                    result.isAccepted() ? "Accepted" : "Rejected", result.getReason());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("[HashAllocationController] Failed to process hash proposal: {}", e.getMessage(), e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("accepted", false);
            errorResponse.put("reason", "Processing error: " + e.getMessage());
            errorResponse.put("error", true);
            
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    /**
     * Handle hash confirmation requests
     * @param request Hash confirmation request
     * @return Confirmation response
     */
    @PostMapping("/confirmation")
    public ResponseEntity<Map<String, Object>> handleHashConfirmation(@RequestBody NodeJoinRequest request) {
        log.info("[HashAllocationController] 📥 Received hash confirmation via HTTP: {} from: {}", 
                request.getProposedHash(), request.getNodeUrl());
        
        try {
            dynamicHashAllocator.processHashConfirmationHTTP(request);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "confirmed");
            response.put("message", "Hash allocation confirmed");
            response.put("hash", request.getProposedHash());
            response.put("node", request.getNodeUrl());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("[HashAllocationController] Failed to process hash confirmation: {}", e.getMessage(), e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Confirmation processing failed: " + e.getMessage());
            
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    /**
     * Health check for hash allocation service
     * @return Service status
     */
    @PostMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "healthy");
        health.put("service", "hash-allocation");
        health.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.ok(health);
    }
    
    /**
     * Proposal result wrapper class
     */
    public static class ProposalResult {
        private boolean accepted;
        private String reason;
        private String respondingNode;
        
        public ProposalResult(boolean accepted, String reason, String respondingNode) {
            this.accepted = accepted;
            this.reason = reason;
            this.respondingNode = respondingNode;
        }
        
        public boolean isAccepted() { return accepted; }
        public String getReason() { return reason; }
        public String getRespondingNode() { return respondingNode; }
    }
} 