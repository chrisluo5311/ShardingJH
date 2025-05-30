package org.distributed.shardingjh.gossip;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class InitialGossipStarter implements ApplicationRunner {

    @Resource
    GossipService gossipService;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Resource
    FingerTable fingerTable;

    /**
     * Simulate new node joining the network by sending an initial gossip message.
     * Steps:
     * 1. Check local finger table
     * 2. If current node is not in the finger table
     * 3. Add current node to the finger table
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
            // Add current node to finger table
            int NEW_NODE_HASH_KEY = 224;
            fingerTable.finger.put(NEW_NODE_HASH_KEY, CURRENT_NODE_URL);
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
}
