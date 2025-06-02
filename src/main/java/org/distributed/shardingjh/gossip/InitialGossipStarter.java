package org.distributed.shardingjh.gossip;

import java.util.ArrayList;
import java.util.Map;

import org.distributed.shardingjh.common.constant.ShardConst;
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
            log.info("[sendInitialGossip] Current node {} is not in the finger table. Adding to finger table", CURRENT_NODE_URL);
            
            // Find the correct hash for current node from configuration
            currentNodeHash = findCurrentNodeHashFromConfig();
            if (currentNodeHash != null) {
                fingerTable.finger.put(currentNodeHash, CURRENT_NODE_URL);
                log.info("[sendInitialGossip] Added current node with hash {} from configuration", currentNodeHash);
            } else {
                // Calculate hash dynamically based on node URL
                currentNodeHash = Math.abs(CURRENT_NODE_URL.hashCode()) % ShardConst.FINGER_MAX_RANGE;
                fingerTable.finger.put(currentNodeHash, CURRENT_NODE_URL);
                log.info("[sendInitialGossip] Could not find current node in configuration, using calculated hash {} for node {}", currentNodeHash, CURRENT_NODE_URL);
            }
        }

        // Always send gossip message to announce our presence to other nodes
        // Other nodes may have removed us from their finger tables due to previous failures
        log.info("[sendInitialGossip] Sending gossip to announce node {} (hash: {}) is online", CURRENT_NODE_URL, currentNodeHash);
        
        // Create gossip message with unique identifier
        GossipMsg gossipMsg = GossipMsg.builder()
                .msgType(GossipMsg.Type.HOST_ADD)
                .msgContent(fingerTable.finger.toString())
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();

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
