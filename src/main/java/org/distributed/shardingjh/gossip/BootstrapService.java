package org.distributed.shardingjh.gossip;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

/**
 * Bootstrap service for node discovery before hash allocation
 * Handles the chicken-and-egg problem: nodes need to know other nodes to allocate hash,
 * but other nodes need to know them to send gossip.
 */
@Slf4j
@Service
public class BootstrapService {
    
    @Resource
    private FingerTable fingerTable;
    
    @Resource
    @Lazy
    private GossipService gossipService;
    
    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;
    
    @Value("${finger.entries}")
    private String entries;
    
    // Bootstrap nodes cache - nodes that don't have hash yet but are trying to join
    private final ConcurrentHashMap<String, Long> bootstrapNodes = new ConcurrentHashMap<>();
    
    // Failed nodes cache - nodes that have been detected as down
    private final ConcurrentHashMap<String, Long> failedNodes = new ConcurrentHashMap<>();
    
    // Timeout for bootstrap nodes (30 seconds)
    private static final long BOOTSTRAP_TIMEOUT_MS = 30000;
    
    // Timeout for failed nodes (60 seconds) - longer than bootstrap to avoid immediate retry
    private static final long FAILED_NODE_TIMEOUT_MS = 60000;
    
    // Scheduler for cleanup tasks
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    @PostConstruct
    public void init() {
        // Schedule cleanup task every 30 seconds
        scheduler.scheduleAtFixedRate(this::cleanupExpiredBootstrapNodes, 30, 30, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::cleanupExpiredFailedNodes, 60, 60, TimeUnit.SECONDS);
    }
    
    /**
     * Register current node as bootstrapping (before getting hash)
     */
    public void registerBootstrapNode() {
        bootstrapNodes.put(CURRENT_NODE_URL, System.currentTimeMillis());
        log.info("[BootstrapService] Registered current node as bootstrapping: {}", CURRENT_NODE_URL);
        
        // Send bootstrap announcement to configured nodes
        announceBootstrapToConfiguredNodes();
    }
    
    /**
     * Unregister current node from bootstrap (after getting hash)
     */
    public void unregisterBootstrapNode() {
        bootstrapNodes.remove(CURRENT_NODE_URL);
        log.info("[BootstrapService] Unregistered current node from bootstrap: {}", CURRENT_NODE_URL);
    }
    
    /**
     * Get all known nodes (finger table + bootstrap nodes)
     * This ensures that even nodes without hash can discover the network
     */
    public List<String> getAllKnownNodes() {
        Set<String> allNodes = new HashSet<>();
        
        // Add nodes from finger table
        allNodes.addAll(fingerTable.finger.values());
        
        // Add nodes from configuration (even if not in finger table yet)
        addConfiguredNodes(allNodes);
        
        // Add bootstrap nodes
        allNodes.addAll(bootstrapNodes.keySet());
        
        // Remove current node
        allNodes.remove(CURRENT_NODE_URL);
        
        // Remove failed nodes
        allNodes.removeIf(nodeUrl -> {
            boolean isFailed = isNodeFailed(nodeUrl);
            if (isFailed) {
                log.debug("[BootstrapService] Excluding failed node from known nodes: {}", nodeUrl);
            }
            return isFailed;
        });
        
        List<String> result = new ArrayList<>(allNodes);
        log.debug("[BootstrapService] All known nodes (excluding {} failed): {}", 
                 failedNodes.size(), result);
        return result;
    }
    
    /**
     * Add configured nodes from finger.entries
     */
    private void addConfiguredNodes(Set<String> nodes) {
        for (String entry : entries.split(",")) {
            String[] parts = entry.split("=");
            if (parts.length == 2) {
                nodes.add(parts[1]);
            }
        }
    }
    
    /**
     * Announce bootstrap to configured nodes
     */
    private void announceBootstrapToConfiguredNodes() {
        Set<String> configuredNodes = new HashSet<>();
        addConfiguredNodes(configuredNodes);
        configuredNodes.remove(CURRENT_NODE_URL);
        
        if (configuredNodes.isEmpty()) {
            log.warn("[BootstrapService] No configured nodes found for bootstrap announcement");
            return;
        }
        
        // Create bootstrap announcement message
        GossipMsg bootstrapMsg = GossipMsg.builder()
                .msgType(GossipMsg.Type.NODE_JOIN)
                .msgContent("BOOTSTRAP:" + CURRENT_NODE_URL)
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();
        
        log.info("[BootstrapService] Announcing bootstrap to configured nodes: {}", configuredNodes);
        gossipService.randomSendGossip(bootstrapMsg, new ArrayList<>(configuredNodes));
    }
    
    /**
     * Handle bootstrap announcement from other nodes
     */
    public void handleBootstrapAnnouncement(String nodeUrl) {
        if (!nodeUrl.equals(CURRENT_NODE_URL)) {
            bootstrapNodes.put(nodeUrl, System.currentTimeMillis());
            log.info("[BootstrapService] Received bootstrap announcement from: {}", nodeUrl);
            
            // Send current finger table back to help the new node discover network
            sendFingerTableSnapshot(nodeUrl);
        }
    }
    
    /**
     * Send finger table snapshot to help new node discover network
     */
    private void sendFingerTableSnapshot(String targetNode) {
        if (fingerTable.finger.isEmpty()) {
            log.debug("[BootstrapService] No finger table to share with: {}", targetNode);
            return;
        }
        
        GossipMsg snapshotMsg = GossipMsg.builder()
                .msgType(GossipMsg.Type.HOST_ADD)
                .msgContent(fingerTable.finger.toString())
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();
        
        log.info("[BootstrapService] Sending finger table snapshot to: {}", targetNode);
        gossipService.randomSendGossip(snapshotMsg, List.of(targetNode));
    }
    
    /**
     * Clean up expired bootstrap nodes
     */
    public void cleanupExpiredBootstrapNodes() {
        long now = System.currentTimeMillis();
        bootstrapNodes.entrySet().removeIf(entry -> {
            boolean expired = (now - entry.getValue()) > BOOTSTRAP_TIMEOUT_MS;
            if (expired) {
                log.info("[BootstrapService] Cleaning up expired bootstrap node: {}", entry.getKey());
            }
            return expired;
        });
    }
    
    /**
     * Check if a node is in bootstrap mode
     */
    public boolean isBootstrapNode(String nodeUrl) {
        return bootstrapNodes.containsKey(nodeUrl);
    }
    
    /**
     * Get count of active bootstrap nodes
     */
    public int getBootstrapNodeCount() {
        return bootstrapNodes.size();
    }
    
    /**
     * Get configured entries from finger.entries
     * @return List of configuration entries (format: "hash=url")
     */
    public List<String> getConfiguredEntries() {
        List<String> entries = new ArrayList<>();
        if (this.entries != null && !this.entries.trim().isEmpty()) {
            String[] entryArray = this.entries.split(",");
            for (String entry : entryArray) {
                if (entry != null && entry.trim().contains("=")) {
                    entries.add(entry.trim());
                }
            }
        }
        return entries;
    }
    
    /**
     * Mark a node as failed/down
     */
    public void markNodeAsFailed(String nodeUrl) {
        failedNodes.put(nodeUrl, System.currentTimeMillis());
        log.info("[BootstrapService] Marked node as failed: {}", nodeUrl);
    }
    
    /**
     * Check if a node is marked as failed
     */
    public boolean isNodeFailed(String nodeUrl) {
        return failedNodes.containsKey(nodeUrl);
    }
    
    /**
     * Remove a node from failed list (if it comes back online)
     */
    public void markNodeAsActive(String nodeUrl) {
        failedNodes.remove(nodeUrl);
        log.info("[BootstrapService] Marked node as active: {}", nodeUrl);
    }
    
    /**
     * Clean up expired failed nodes
     */
    private void cleanupExpiredFailedNodes() {
        long now = System.currentTimeMillis();
        failedNodes.entrySet().removeIf(entry -> {
            boolean expired = (now - entry.getValue()) > FAILED_NODE_TIMEOUT_MS;
            if (expired) {
                log.info("[BootstrapService] Cleaning up expired failed node: {}", entry.getKey());
            }
            return expired;
        });
    }
} 