package org.distributed.shardingjh.gossip;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

/**
 * HTTP Client for Hash Allocation - Provides reliable communication for hash allocation
 */
@Slf4j
@Service
public class HashAllocationHTTPClient {
    
    @Value("${hash-allocation.timeout:5000}")
    private int timeoutMs;
    
    @Value("${hash-allocation.connect-timeout:3000}")
    private int connectTimeoutMs;
    
    private RestTemplate restTemplate;
    
    @PostConstruct
    public void initRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(timeoutMs);
        
        this.restTemplate = new RestTemplateBuilder()
                .requestFactory(() -> factory)
                .build();
        
        log.info("[HashAllocationHTTPClient] RestTemplate initialized with connect timeout: {}ms, read timeout: {}ms", 
                connectTimeoutMs, timeoutMs);
    }
    
    /**
     * Send hash proposal to target node
     * @param targetNodeUrl Target node URL
     * @param request Hash proposal request
     * @return Proposal response
     */
    public ProposalResponse sendHashProposal(String targetNodeUrl, NodeJoinRequest request) {
        return sendHashProposalWithRetries(targetNodeUrl, request, 3);
    }
    
    /**
     * Send hash proposal with retry mechanism
     * @param targetNodeUrl Target node URL
     * @param request Hash proposal request
     * @param maxRetries Maximum retry attempts
     * @return Proposal response
     */
    public ProposalResponse sendHashProposalWithRetries(String targetNodeUrl, NodeJoinRequest request, int maxRetries) {
        String endpoint = "/hash-allocation/proposal";
        String fullUrl = targetNodeUrl + endpoint;
        
        log.info("[HashAllocationHTTPClient] 🚀 Sending hash proposal to {}: hash={}", targetNodeUrl, request.getProposedHash());
        
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // Set request headers
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                headers.set("X-Hash-Allocation-From", request.getNodeUrl());
                
                HttpEntity<NodeJoinRequest> requestEntity = new HttpEntity<>(request, headers);
                
                // Send proposal request
                ResponseEntity<Map> response = restTemplate.exchange(
                        fullUrl,
                        HttpMethod.POST,
                        requestEntity,
                        Map.class
                );
                
                if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                    Map<String, Object> responseBody = response.getBody();
                    Boolean accepted = (Boolean) responseBody.get("accepted");
                    String reason = (String) responseBody.get("reason");
                    String respondingNode = (String) responseBody.get("respondingNode");
                    
                    log.info("[HashAllocationHTTPClient] ✅ Received proposal response from {}: {} ({})", 
                            targetNodeUrl, accepted ? "Accepted" : "Rejected", reason);
                    
                    return new ProposalResponse(accepted != null ? accepted : false, reason, respondingNode);
                } else {
                    log.warn("[HashAllocationHTTPClient] Abnormal response from {}, status: {}", 
                            targetNodeUrl, response.getStatusCode());
                    lastException = new RuntimeException("Abnormal response status: " + response.getStatusCode());
                }
                
            } catch (Exception e) {
                lastException = e;
                if (attempt < maxRetries) {
                    log.warn("[HashAllocationHTTPClient] Attempt {}/{} failed to send proposal to {}: {}, retrying...", 
                            attempt, maxRetries, targetNodeUrl, e.getMessage());
                    try {
                        Thread.sleep(100 * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    log.error("[HashAllocationHTTPClient] All {} attempts failed to send proposal to {}: {}", 
                             maxRetries, targetNodeUrl, e.getMessage());
                }
            }
        }
        
        // If all retries failed, return rejection
        String errorMessage = lastException != null ? lastException.getMessage() : "Unknown error";
        log.error("[HashAllocationHTTPClient] ❌ Failed to send proposal to {} after {} attempts: {}", 
                 targetNodeUrl, maxRetries, errorMessage);
        
        return new ProposalResponse(false, "Communication failed: " + errorMessage, null);
    }
    
    /**
     * Send hash confirmation to target node (async)
     * @param targetNodeUrl Target node URL
     * @param request Hash confirmation request
     * @return Future result
     */
    @Async
    public CompletableFuture<Boolean> sendHashConfirmationAsync(String targetNodeUrl, NodeJoinRequest request) {
        try {
            boolean result = sendHashConfirmation(targetNodeUrl, request);
            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            log.error("[HashAllocationHTTPClient] Failed to send confirmation to {}: {}", targetNodeUrl, e.getMessage());
            return CompletableFuture.completedFuture(false);
        }
    }
    
    /**
     * Send hash confirmation to target node
     * @param targetNodeUrl Target node URL
     * @param request Hash confirmation request
     * @return Success status
     */
    public boolean sendHashConfirmation(String targetNodeUrl, NodeJoinRequest request) {
        String endpoint = "/hash-allocation/confirmation";
        String fullUrl = targetNodeUrl + endpoint;
        
        log.info("[HashAllocationHTTPClient] 🚀 Sending hash confirmation to {}: hash={}", 
                targetNodeUrl, request.getProposedHash());
        
        try {
            // Set request headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("X-Hash-Allocation-From", request.getNodeUrl());
            
            HttpEntity<NodeJoinRequest> requestEntity = new HttpEntity<>(request, headers);
            
            // Send confirmation request
            ResponseEntity<Map> response = restTemplate.exchange(
                    fullUrl,
                    HttpMethod.POST,
                    requestEntity,
                    Map.class
            );
            
            if (response.getStatusCode() == HttpStatus.OK) {
                log.info("[HashAllocationHTTPClient] ✅ Successfully sent confirmation to {}", targetNodeUrl);
                return true;
            } else {
                log.warn("[HashAllocationHTTPClient] Abnormal confirmation response from {}, status: {}", 
                        targetNodeUrl, response.getStatusCode());
                return false;
            }
            
        } catch (Exception e) {
            log.error("[HashAllocationHTTPClient] Failed to send confirmation to {}: {}", targetNodeUrl, e.getMessage());
            return false;
        }
    }
    
    /**
     * Proposal response wrapper
     */
    public static class ProposalResponse {
        private final boolean accepted;
        private final String reason;
        private final String respondingNode;
        
        public ProposalResponse(boolean accepted, String reason, String respondingNode) {
            this.accepted = accepted;
            this.reason = reason;
            this.respondingNode = respondingNode;
        }
        
        public boolean isAccepted() { return accepted; }
        public String getReason() { return reason; }
        public String getRespondingNode() { return respondingNode; }
    }
} 