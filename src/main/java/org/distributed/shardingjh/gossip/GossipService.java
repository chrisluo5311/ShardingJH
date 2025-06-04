package org.distributed.shardingjh.gossip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class GossipService {
    // Message cache for deduplication, key is the unique identifier of the message, value is the receive count
    private final ConcurrentHashMap<String, Integer> msgCache = new ConcurrentHashMap<>();
    private static final int CACHE_SIZE_LIMIT = 1000; // Adjust according to actual needs

    // Track last broadcasted finger table state to avoid unnecessary broadcasts
    private volatile String lastBroadcastedFingerTable = "";
    private volatile long lastBroadcastTime = 0;

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
     * Find hash key by node address in finger table (for duplicate node detection)
     * @param address Node address to find
     * @return Hash key or null if not found
     */
    private Integer findHashByAddress(String address) {
        for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
            if (entry.getValue().equals(address)) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * Clean up duplicate node entries in finger table
     * If a node appears multiple times with different hashes, keep only the first occurrence
     */
    public void cleanupDuplicateNodes() {
        Map<String, Integer> addressToHash = new HashMap<>();
        List<Integer> hashesToRemove = new ArrayList<>();
        
        // Find duplicate addresses
        for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
            String address = entry.getValue();
            Integer hash = entry.getKey();
            
            if (addressToHash.containsKey(address)) {
                // Duplicate found, mark for removal (keep the first occurrence)
                hashesToRemove.add(hash);
                log.warn("[GossipService] Found duplicate node: {} appears with both hash {} and {}. Removing hash {}.", 
                        address, addressToHash.get(address), hash, hash);
            } else {
                addressToHash.put(address, hash);
            }
        }
        
        // Remove duplicate entries
        for (Integer hash : hashesToRemove) {
            String removedAddress = fingerTable.finger.remove(hash);
            log.info("[GossipService] Removed duplicate entry: {}={}", hash, removedAddress);
        }
        
        if (!hashesToRemove.isEmpty()) {
            log.info("[GossipService] Cleanup completed. Removed {} duplicate entries. Current finger table: {}", 
                    hashesToRemove.size(), fingerTable.finger);
        }
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
        
        // For hash allocation messages, use less strict duplicate detection
        // Allow retries for HASH_PROPOSAL and HASH_PROPOSAL_ACK messages
        if (message.getMsgType() == GossipMsg.Type.HASH_PROPOSAL || 
            message.getMsgType() == GossipMsg.Type.HASH_PROPOSAL_ACK) {
            // For critical hash allocation messages, allow up to 3 duplicates
            Integer count = msgCache.getOrDefault(msgKey, 0);
            if (count >= 3) {
                log.info("[GossipService] Too many duplicates for hash allocation message, ignoring: {}", msgKey);
                return;
            }
            msgCache.put(msgKey, count + 1);
        } else {
            // For other messages, use stricter duplicate detection
            Integer count = msgCache.getOrDefault(msgKey, 0);
            if (count >= 1) {
                log.info("[GossipService] Duplicate gossip message received (same sender+timestamp), ignoring: {}", msgKey);
                return;
            }
            msgCache.put(msgKey, count + 1);
        }
        
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
                        
                        // Check for hash conflicts and duplicate node assignments
                        if (fingerTable.finger.containsKey(hash)) {
                            String existingAddress = fingerTable.finger.get(hash);
                            if (!existingAddress.equals(address)) {
                                // Hash conflict detected!
                                if (existingAddress.equals(CURRENT_NODE_URL)) {
                                    log.error("[GossipService] ⚠️ HASH CONFLICT: Hash {} is claimed by both current node {} and remote node {}", 
                                             hash, CURRENT_NODE_URL, address);
                                    // Current node has priority if it's already established
                                    log.warn("[GossipService] Ignoring conflicting hash assignment from {}", address);
                                    continue; // Skip this conflicting entry
                                } else {
                                    log.warn("[GossipService] ⚠️ Hash conflict detected: {} was {} now claims to be {}", 
                                            hash, existingAddress, address);
                                    // Update to newer assignment
                                    fingerTable.finger.put(hash, address);
                                    addedAnyNode = true;
                                    log.info("[GossipService] Updated conflicting hash assignment: {}={}", hash, address);
                                }
                            }
                            // If existing address equals new address, no change needed
                        } else {
                            // Check if this node already exists with a different hash (duplicate node detection)
                            Integer existingHash = findHashByAddress(address);
                            if (existingHash != null && !existingHash.equals(hash)) {
                                log.warn("[GossipService] ⚠️ DUPLICATE NODE DETECTED: Node {} already exists with hash {}, ignoring new hash {}", 
                                        address, existingHash, hash);
                                continue; // Skip duplicate node assignment
                            }
                            
                            fingerTable.addEntry(hash, address);
                            log.info("[GossipService] Added host to finger table: {}={}", hash, address);
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
                    log.info("[GossipService] Received dynamic hash allocation message: {} from sender: {}", 
                             message.getMsgType(), message.getSenderId());
                    
                    // Special handling for bootstrap announcements
                    if (message.getMsgType() == GossipMsg.Type.NODE_JOIN && 
                        message.getMsgContent().startsWith("BOOTSTRAP:")) {
                        String bootstrapNode = message.getMsgContent().substring("BOOTSTRAP:".length());
                        bootstrapService.handleBootstrapAnnouncement(bootstrapNode);
                    } else {
                        // Add more detailed logging for hash allocation messages
                        log.info("[GossipService] Processing hash allocation message content: {}", message.getMsgContent());
                        try {
                            dynamicHashAllocator.handleHashAllocationGossip(message);
                            log.info("[GossipService] Successfully processed hash allocation message: {}", message.getMsgType());
                        } catch (Exception e) {
                            log.error("[GossipService] Failed to process hash allocation message: {}", e.getMessage(), e);
                        }
                    }
                    // These messages don't need further propagation, as DynamicHashAllocator handles propagation logic
                    }
                    break;
            }
            
            if (gossipMsg != null) {
                randomSendGossip(gossipMsg, new ArrayList<>(fingerTable.finger.values()));
            }
        } catch (Exception e) {
            log.error("[GossipService] Failed to handle gossip message: {}", e.getMessage(), e);
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

        // Determine if this is a critical hash allocation message that needs retries
        boolean isCriticalMessage = gossipMsg.getMsgType() == GossipMsg.Type.HASH_PROPOSAL ||
                                   gossipMsg.getMsgType() == GossipMsg.Type.HASH_PROPOSAL_ACK ||
                                   gossipMsg.getMsgType() == GossipMsg.Type.HASH_CONFIRMATION;
        
        int retries = isCriticalMessage ? 2 : 1; // Use retries for critical messages
        
        // Send to ALL valid neighbors (not just 2 random ones)
        log.info("[GossipService] Sending gossip message to ALL {} neighbors{}", 
                validNeighbors.size(), isCriticalMessage ? " with retries" : "");
        for (String neighborUrl : validNeighbors) {
            String[] partsNeighbor = neighborUrl.split(":");
            String neighborIp = partsNeighbor[1].replace("//", "");
            
            log.info("[GossipService] Sending gossip message to neighbor: {}", neighborIp);
            try {
                if (isCriticalMessage) {
                    gossipSender.sendGossipWithRetries(gossipMsg, neighborIp, PORT, retries);
                } else {
                    gossipSender.sendGossip(gossipMsg, neighborIp, PORT);
                }
                log.debug("[GossipService] Successfully sent gossip message to: {}", neighborIp);
            } catch (Exception e) {
                log.error("[GossipService] Failed to send gossip message to {}: {}", neighborIp, e.getMessage());
            }
        }
    }

    /**
    Periodically broadcast membership info to all nodes
     */
    @Scheduled(fixedRate = 15000)
    public void periodicGossipMembershipInfo() {
        if (fingerTable.finger.isEmpty()) {
            log.debug("[GossipService] Finger table is empty, skip periodic gossip.");
            return;
        }
        
        String currentFingerTableState = fingerTable.finger.toString();
        long currentTime = System.currentTimeMillis();
        
        // Check if finger table has changed since last broadcast or if it's been more than 60 seconds
        // This prevents unnecessary broadcast storms while ensuring periodic health checks
        boolean shouldBroadcast = !currentFingerTableState.equals(lastBroadcastedFingerTable) || 
                                 (currentTime - lastBroadcastTime) > 60000; // Force broadcast every 60 seconds
        
        if (!shouldBroadcast) {
            log.debug("[GossipService] Finger table unchanged since last broadcast, skipping to reduce gossip storm");
            return;
        }
        
        lastBroadcastedFingerTable = currentFingerTableState;
        lastBroadcastTime = currentTime;
        
        GossipMsg membershipMsg = GossipMsg.builder()
                .msgType(GossipMsg.Type.HOST_ADD)
                .msgContent(currentFingerTableState)
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(currentTime))
                .build();
        log.info("[GossipService] Periodically broadcasting membership info: {}", fingerTable.finger);
        randomSendGossip(membershipMsg, new ArrayList<>(fingerTable.finger.values()));
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



