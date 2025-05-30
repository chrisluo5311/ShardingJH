package org.distributed.shardingjh.gossip;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.p2p.FingerTable;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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
        // Check if the message has already been received (e.g., "HOST_ADD_64=http://18.222.111.89:8081")
        String msgKey = message.getMsgType() + "_" + message.getMsgContent();
        log.info("[GossipService] Received gossip message key: {}", msgKey);
        Integer count = msgCache.getOrDefault(msgKey, 0);
        if (count >= 1) {
            log.info("[GossipService] Duplicate gossip message received, ignoring: {}", msgKey);
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
                    fingerTable.finger.remove(hash);
                    gossipMsg = GossipMsg.builder()
                                        .msgType(GossipMsg.Type.HOST_DOWN)
                                        .msgContent(fingerTable.finger.toString())
                                        .build();
                    }
                    break;
                case HOST_ADD: {
                    // Add host to finger table
                    // {64=http://localhost:8081, 224=http://localhost:8084}
                    log.info("[GossipService] Received HOST_ADD gossip message: {}", message.getMsgContent());
                    String cleaned = message.getMsgContent().replaceAll("[\\{\\} ]", "");
                    String[] parts = cleaned.split(",");
                    for (String part: parts) {
                        String[] eachParts = part.split("=");
                        int hash = Integer.parseInt(eachParts[0]);
                        String address = eachParts[1];
                        if (!fingerTable.finger.containsKey(hash)) {
                            fingerTable.addEntry(hash,address);
                            log.info("[GossipService] Added host to finger table: {}={}", hash, address);
                        }
                    }
                    gossipMsg = GossipMsg.builder()
                                        .msgType(GossipMsg.Type.HOST_ADD)
                                        .msgContent(fingerTable.finger.toString())
                                        .build();
                    }
                    break;
            }
            randomSendGossip(gossipMsg, new ArrayList<>(fingerTable.finger.values()));
        } catch (Exception e) {
            log.error("[GossipService] Failed to handle gossip message: {}", e.getMessage());
        }
    }

    public void randomSendGossip(GossipMsg gossipMsg, List<String> neighbors) {
        if (neighbors.isEmpty()) {
            log.warn("[GossipService] No neighbors to send gossip message to.");
            return;
        }

        // Randomly select 2 neighbors
        Collections.shuffle(neighbors);
        int pickCount = Math.min(2, neighbors.size());
        Set<String> uniqueNeighbors = new HashSet<>();

        while (pickCount > 0) {
            String neighborUrl = neighbors.get(new Random().nextInt(neighbors.size()));
            String[] partsNeighbor = neighborUrl.split(":");
            String neighborIp = partsNeighbor[1].replace("//", "");
//            int port = Integer.parseInt(partsNeighbor[2]);
            log.info("[GossipService] Sending gossip message to neighbor: {}", neighborIp);
            if (!neighborIp.equals(getCurrentIp()) && !uniqueNeighbors.contains(neighborUrl)) {
                gossipSender.sendGossip(gossipMsg, neighborIp, PORT);
                uniqueNeighbors.add(neighborUrl);
                pickCount--;
            }

            // For testing purposes
//            if (port == 8081) {
//                gossipSender.sendGossip(gossipMsg, neighborIp, 9000);
//                uniqueNeighbors.add(neighborUrl);
//                pickCount--;
//            }
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



