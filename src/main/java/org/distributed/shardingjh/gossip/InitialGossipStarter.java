package org.distributed.shardingjh.gossip;

import java.util.ArrayList;
import java.util.Map;

import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class InitialGossipStarter implements ApplicationRunner {

    @Resource
    GossipService gossipService;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Value("${finger.entries}")
    private String entries;

    @Resource
    FingerTable fingerTable;

    @Resource
    DynamicHashAllocator dynamicHashAllocator;

    /**
     * Simulate new node joining the network by sending an initial gossip message.
     * Steps:
     * 1. Check local finger table
     * 2. If current node is not in the finger table, add it with correct hash
     * 3. Always send gossip message to notify other nodes of our presence (they may have removed us)
     *
     * */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        boolean isCurrentNodeInFingerTable = false;
        Integer currentNodeHash = null;
        
        // Check if current node is in finger table and get its hash
        for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
            log.info("[sendInitialGossip] Checking finger table entry: {} -> {}", entry.getKey(), entry.getValue());
            if (entry.getValue().equals(CURRENT_NODE_URL)) {
                isCurrentNodeInFingerTable = true;
                currentNodeHash = entry.getKey();
                log.info("[sendInitialGossip] Current node {} found in finger table with hash {}", CURRENT_NODE_URL, currentNodeHash);
                break;
            }
        }

        if (!isCurrentNodeInFingerTable) {
            log.info("[sendInitialGossip] Current node {} is not in the finger table. Starting dynamic hash allocation", CURRENT_NODE_URL);
            
            // First try to get hash from configuration
            currentNodeHash = findCurrentNodeHashFromConfig();
            if (currentNodeHash != null) {
                // Verify config hash is not occupied by a different node
                String existingNode = fingerTable.finger.get(currentNodeHash);
                if (existingNode != null && !existingNode.equals(CURRENT_NODE_URL)) {
                    log.error("[sendInitialGossip] Hash collision detected! Hash {} is configured for current node {} but occupied by {}", 
                             currentNodeHash, CURRENT_NODE_URL, existingNode);
                    throw new RuntimeException("Configuration error: Hash " + currentNodeHash + " is already occupied by " + existingNode);
                }
                fingerTable.finger.put(currentNodeHash, CURRENT_NODE_URL);
                log.info("[sendInitialGossip] Added current node with hash {} from configuration", currentNodeHash);
            } else {
                // Use dynamic hash allocator
                try {
                    log.info("[sendInitialGossip] No configuration found, using dynamic hash allocation via gossip");
                    currentNodeHash = dynamicHashAllocator.allocateHashForCurrentNode();
                    log.info("[sendInitialGossip] Dynamic hash allocation completed with hash: {}", currentNodeHash);
                    // Note: DynamicHashAllocator already adds the node to finger table
                } catch (InterruptedException e) {
                    log.error("[sendInitialGossip] Dynamic hash allocation was interrupted: {}", e.getMessage());
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Dynamic hash allocation was interrupted", e);
                } catch (Exception e) {
                    log.error("[sendInitialGossip] Dynamic hash allocation failed: {}", e.getMessage());
                    throw new RuntimeException("Dynamic hash allocation failed", e);
                }
            }
        }

        // Always send gossip message to announce our presence to other nodes
        // Other nodes may have removed us from their finger tables due to previous failures
        log.info("[sendInitialGossip] Sending gossip to announce node {} (hash: {}) is online", CURRENT_NODE_URL, currentNodeHash);
        
        // Verify current node is in finger table before sending gossip
        log.info("[sendInitialGossip] Current finger table before sending gossip: {}", fingerTable.finger);
        
        // Create gossip message with unique identifier
        GossipMsg gossipMsg = GossipMsg.builder()
                .msgType(GossipMsg.Type.HOST_ADD)
                .msgContent(fingerTable.finger.toString())
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();

        log.info("[sendInitialGossip] Sending HOST_ADD message with content: {}", gossipMsg.getMsgContent());

        // Send gossip message to 2 random selections of neighbors
        int round = 2;
        for (int i = 0 ; i < round ; i++) {
            log.info("[sendInitialGossip] Sending Gossip round {}/{} to announce presence", i+1, round);
            gossipService.randomSendGossip(gossipMsg, new ArrayList<>(fingerTable.finger.values()));
        }
    }
    
    /**
     * Find the hash key for current node from finger.entries configuration
     * @return Hash key or null if not found
     */
    private Integer findCurrentNodeHashFromConfig() {
        for (String entry : entries.split(",")) {
            String[] parts = entry.split("=");
            if (parts.length == 2 && parts[1].equals(CURRENT_NODE_URL)) {
                return Integer.parseInt(parts[0]);
            }
        }
        return null;
    }
}
