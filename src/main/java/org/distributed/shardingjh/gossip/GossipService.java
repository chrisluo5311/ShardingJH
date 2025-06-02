package org.distributed.shardingjh.gossip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class GossipService {
    // Message cache for deduplication, key is the unique identifier of the message, value is the receive count
    private final ConcurrentHashMap<String, Integer> msgCache = new ConcurrentHashMap<>();
    private static final int CACHE_SIZE_LIMIT = 1000; // Adjust according to actual needs

    @Resource
    private FingerTable fingerTable;

    @Resource
    GossipSender gossipSender;

    @Resource
    @Lazy
    DynamicHashAllocator dynamicHashAllocator;

    @Resource
    @Lazy
    BootstrapService bootstrapService;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Value("${gossip.port}")
    private int PORT;

    // http://3.15.149.110:8082 => 3.15.149.110
    public String getCurrentIp() {
        return CURRENT_NODE_URL.split(":")[1].replace("//", "");
    }

    /**
     * Handle received gossip messages.
     *
     * */
    public void msgHandle(GossipMsg message) {
        // Use senderId + timestamp for duplicate detection instead of message content
        // This allows same content messages from different senders or different times to be processed
        String msgKey = message.getSenderId() + "_" + message.getTimestamp() + "_" + message.getMsgType();
        log.info("[GossipService] Received gossip message key: {}", msgKey);
        
        // Check for duplicate based on sender + timestamp + type
        Integer count = msgCache.getOrDefault(msgKey, 0);
        if (count >= 1) {
            log.info("[GossipService] Duplicate gossip message received (same sender+timestamp), ignoring: {}", msgKey);
            return;
        }
        msgCache.put(msgKey, count + 1);
        
        if (msgCache.size() > CACHE_SIZE_LIMIT) {
            msgCache.clear();
        }

        // Handle the gossip message based on its type
        try {
            GossipMsg gossipMsg = null;
            switch (message.getMsgType()) {
                case HOST_DOWN: {
                    // Remove host from finger table
                    int hash = Integer.parseInt(message.getMsgContent());
                    String removedUrl = fingerTable.finger.remove(hash);
                    if (removedUrl != null) {
                        log.info("[GossipService] Removed host from finger table: {}={}", hash, removedUrl);
                        // Propagate with original sender info to maintain proper duplicate detection
                        gossipMsg = GossipMsg.builder()
                                            .msgType(GossipMsg.Type.HOST_DOWN)
                                            .msgContent(fingerTable.finger.toString())
                                            .senderId(message.getSenderId())  // Keep original sender
                                            .timestamp(message.getTimestamp())  // Keep original timestamp
                                            .build();
                    } else {
                        log.info("[GossipService] Host {} was not in finger table, no changes made", hash);
                    }
                    }
                    break;
                case HOST_ADD: {
                    // Add host to finger table
                    // {64=http://localhost:8081, 224=http://localhost:8084}
                    log.info("[GossipService] Received HOST_ADD gossip message: {}", message.getMsgContent());
                    String cleaned = message.getMsgContent().replaceAll("[\\{\\} ]", "");
                    String[] parts = cleaned.split(",");
                    boolean addedAnyNode = false;
                    for (String part: parts) {
                        String[] eachParts = part.split("=");
                        int hash = Integer.parseInt(eachParts[0]);
                        String address = eachParts[1];
                        if (!fingerTable.finger.containsKey(hash)) {
                            fingerTable.addEntry(hash,address);
                            log.info("[GossipService] Added host to finger table: {}={}", hash, address);
                            addedAnyNode = true;
                        } else if (!fingerTable.finger.get(hash).equals(address)) {
                            // Update if the address changed for the same hash
                            fingerTable.finger.put(hash, address);
                            log.info("[GossipService] Updated host in finger table: {}={}", hash, address);
                            addedAnyNode = true;
                        }
                    }
                    
                    // Only propagate if we actually made changes to our finger table
                    if (addedAnyNode) {
                        // Propagate with original sender info to maintain proper duplicate detection
                        gossipMsg = GossipMsg.builder()
                                            .msgType(GossipMsg.Type.HOST_ADD)
                                            .msgContent(fingerTable.finger.toString())
                                            .senderId(message.getSenderId())  // Keep original sender
                                            .timestamp(message.getTimestamp())  // Keep original timestamp
                                            .build();
                        log.info("[GossipService] Made changes to finger table, propagating: {}", fingerTable.finger);
                    } else {
                        log.info("[GossipService] No changes to finger table, not propagating to avoid gossip storm");
                    }
                    }
                    break;
                case NODE_JOIN:
                case HASH_PROPOSAL:
                case HASH_PROPOSAL_ACK:
                case HASH_CONFIRMATION: {
                    // Handle dynamic hash allocation related messages
                    log.info("[GossipService] Received dynamic hash allocation message: {}", message.getMsgType());
                    
                    // Special handling for bootstrap announcements
                    if (message.getMsgType() == GossipMsg.Type.NODE_JOIN && 
                        message.getMsgContent().startsWith("BOOTSTRAP:")) {
                        String bootstrapNode = message.getMsgContent().substring("BOOTSTRAP:".length());
                        bootstrapService.handleBootstrapAnnouncement(bootstrapNode);
                    } else {
                        dynamicHashAllocator.handleHashAllocationGossip(message);
                    }
                    // These messages don't need further propagation, as DynamicHashAllocator handles propagation logic
                    }
                    break;
            }
            
            if (gossipMsg != null) {
                randomSendGossip(gossipMsg, new ArrayList<>(fingerTable.finger.values()));
            }
        } catch (Exception e) {
            log.error("[GossipService] Failed to handle gossip message: {}", e.getMessage());
        }
    }

    public void randomSendGossip(GossipMsg gossipMsg, List<String> neighbors) {
        if (neighbors.isEmpty()) {
            log.warn("[GossipService] No neighbors to send gossip message to.");
            return;
        }

        // Filter out current node from neighbors first
        List<String> validNeighbors = new ArrayList<>();
        String currentIp = getCurrentIp();
        
        for (String neighborUrl : neighbors) {
            String[] partsNeighbor = neighborUrl.split(":");
            String neighborIp = partsNeighbor[1].replace("//", "");
            if (!neighborIp.equals(currentIp)) {
                validNeighbors.add(neighborUrl);
            }
        }
        
        if (validNeighbors.isEmpty()) {
            log.warn("[GossipService] No valid neighbors to send gossip message to (all are current node).");
            return;
        }

        // Randomly select up to 2 unique neighbors
        Collections.shuffle(validNeighbors);
        int pickCount = Math.min(2, validNeighbors.size());
        
        for (int i = 0; i < pickCount; i++) {
            String neighborUrl = validNeighbors.get(i);
            String[] partsNeighbor = neighborUrl.split(":");
            String neighborIp = partsNeighbor[1].replace("//", "");
            
            log.info("[GossipService] Sending gossip message to neighbor: {}", neighborIp);
            try {
                gossipSender.sendGossip(gossipMsg, neighborIp, PORT);
                log.debug("[GossipService] Successfully sent gossip message to: {}", neighborIp);
            } catch (Exception e) {
                log.error("[GossipService] Failed to send gossip message to {}: {}", neighborIp, e.getMessage());
            }
        }
    }

    /**
     * Send gossip message to ALL specified nodes (for critical messages like HASH_PROPOSAL)
     * Unlike randomSendGossip which only sends to 2 random nodes, this sends to all
     */
    public void sendToAllNodes(GossipMsg gossipMsg, List<String> neighbors) {
        if (neighbors.isEmpty()) {
            log.warn("[GossipService] No neighbors to send gossip message to.");
            return;
        }

        // Filter out current node from neighbors first
        List<String> validNeighbors = new ArrayList<>();
        String currentIp = getCurrentIp();
        
        for (String neighborUrl : neighbors) {
            String[] partsNeighbor = neighborUrl.split(":");
            String neighborIp = partsNeighbor[1].replace("//", "");
            if (!neighborIp.equals(currentIp)) {
                validNeighbors.add(neighborUrl);
            }
        }
        
        if (validNeighbors.isEmpty()) {
            log.warn("[GossipService] No valid neighbors to send gossip message to (all are current node).");
            return;
        }

        // Send to ALL valid neighbors (not just 2 random ones)
        log.info("[GossipService] Sending gossip message to ALL {} neighbors", validNeighbors.size());
        for (String neighborUrl : validNeighbors) {
            String[] partsNeighbor = neighborUrl.split(":");
            String neighborIp = partsNeighbor[1].replace("//", "");
            
            log.info("[GossipService] Sending gossip message to neighbor: {}", neighborIp);
            try {
                gossipSender.sendGossip(gossipMsg, neighborIp, PORT);
                log.debug("[GossipService] Successfully sent gossip message to: {}", neighborIp);
            } catch (Exception e) {
                log.error("[GossipService] Failed to send gossip message to {}: {}", neighborIp, e.getMessage());
            }
        }
    }

    // Send HOSTUP gossip message
//    public void sendHostUpGossip(GossipNode self, int hash, String address, int numberOfNodes) {
//        GossipMsg msg = new GossipMsg();
//        msg.setMsgType(GossipMsg.Type.HOST_UP.toString());
//        msg.setMsgContent(hash + "=" + address);
//        msg.setSenderId(self.getId());
//        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
//        sendMessage(msg, self.getNeighbors(), numberOfNodes);
//    }
//
//    // Send HOSTDOWN gossip message
//    public void sendHostDownGossip(GossipNode self, int hash, int numberOfNodes) {
//        GossipMsg msg = new GossipMsg();
//        msg.setMsgType(GossipMsg.Type.HOST_DOWN.toString());
//        msg.setMsgContent(String.valueOf(hash));
//        msg.setSenderId(self.getId());
//        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
//        sendMessage(msg, self.getNeighbors(), numberOfNodes);
//    }
//
//    // Send HOSTADD gossip message
//    public void sendHostAddGossip(GossipNode self, int hash, String address, int numberOfNodes) {
//        GossipMsg msg = new GossipMsg();
//        msg.setMsgType(GossipMsg.Type.HOST_ADD.toString());
//        msg.setMsgContent(hash + "=" + address);
//        msg.setSenderId(self.getId());
//        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
//        sendMessage(msg, self.getNeighbors(), numberOfNodes);
//    }
//
//    // Send HOSTREMOVE gossip message
//    public void sendHostRemoveGossip(GossipNode self, int hash, int numberOfNodes) {
//        GossipMsg msg = new GossipMsg();
//        msg.setMsgType(GossipMsg.Type.HOST_REMOVE.toString());
//        msg.setMsgContent(String.valueOf(hash));
//        msg.setSenderId(self.getId());
//        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
//        sendMessage(msg, self.getNeighbors(), numberOfNodes);
//    }

    // Send TABLEUPDATE gossip message to a specific node
//    public void sendTableUpdateGossip(GossipNode self, String receiverId, String tableContent, List<GossipNode> neighbors, int numberOfNodes) {
//        GossipMsg msg = new GossipMsg();
//        msg.setMsgType(GossipMsg.Type.TABLE_UPDATE.toString());
//        msg.setMsgContent(tableContent);
//        msg.setSenderId(self.getId());
//        msg.setReceiverId(receiverId);
//        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
//        sendMessage(msg, neighbors, numberOfNodes);
//    }
}



