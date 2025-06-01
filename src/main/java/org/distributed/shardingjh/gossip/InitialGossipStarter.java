package org.distributed.shardingjh.gossip;

import java.util.ArrayList;

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
     * 2. If current node is not in the finger table
     * 3. Add current node to the finger table with its correct hash from configuration or calculated hash
     * 4. Send gossip message to 2 random nodes in the finger table (except itself)
     *
     * */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        boolean isCurrentNodeInFingerTable = false;
        for (String urls: fingerTable.finger.values()) {
            log.info("[sendInitialGossip] Checking finger table entry: {}", urls);
            if (urls.equals(CURRENT_NODE_URL)) {
                isCurrentNodeInFingerTable = true;
                log.info("[sendInitialGossip] Current node {} is already in the finger table. No Gossip", CURRENT_NODE_URL);
                break;
            }
        }

        if (!isCurrentNodeInFingerTable) {
            log.info("[sendInitialGossip] Current node {} is not in the finger table. Adding to finger table and sending Gossip", CURRENT_NODE_URL);
            
            // Find the correct hash for current node from configuration
            Integer currentNodeHash = findCurrentNodeHashFromConfig();
            if (currentNodeHash != null) {
                fingerTable.finger.put(currentNodeHash, CURRENT_NODE_URL);
                log.info("[sendInitialGossip] Added current node with hash {} from configuration", currentNodeHash);
            } else {
                // Calculate hash dynamically based on node URL
                int calculatedHash = Math.abs(CURRENT_NODE_URL.hashCode()) % ShardConst.FINGER_MAX_RANGE;
                fingerTable.finger.put(calculatedHash, CURRENT_NODE_URL);
                log.info("[sendInitialGossip] Could not find current node in configuration, using calculated hash {} for node {}", calculatedHash, CURRENT_NODE_URL);
            }
            
            // Create gossip message
            GossipMsg gossipMsg = GossipMsg.builder()
                    .msgType(GossipMsg.Type.HOST_ADD)
                    .msgContent(fingerTable.finger.toString())
                    .build();

            // Send gossip message to 2 random selections of neighbors
            int round = 2;
            for (int i = 0 ; i < round ; i++) {
                log.info("[sendInitialGossip] Sending Gossip round {}/{}", i+1, round);
                gossipService.randomSendGossip(gossipMsg, new ArrayList<>(fingerTable.finger.values()));
            }
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
