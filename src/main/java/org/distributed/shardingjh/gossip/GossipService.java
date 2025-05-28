package org.distributed.shardingjh.gossip;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

@Component
public class GossipService {
    // Message cache for deduplication, key is the unique identifier of the message, value is the receive count
    private final ConcurrentHashMap<String, Integer> msgCache = new ConcurrentHashMap<>();
    private static final int CACHE_SIZE_LIMIT = 1000; // Adjust according to actual needs

    @Resource
    private FingerTable fingerTable;

    public void sendMessage(GossipMsg msg, List<GossipNode> neighbors, int numberOfNodes) {
        try{        
            DatagramSocket socket = new DatagramSocket();
            byte[] data = msg.toJson().getBytes(StandardCharsets.UTF_8);
            for (GossipNode neighbor : neighbors) {
                if (neighbor.getStatus().equals("UP")) {
                    InetAddress address = InetAddress.getByName(neighbor.getIp());
                    int port = neighbor.getPort();
                    DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
                    socket.send(packet);
                }
            }
            socket.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void listen(int port) {
        try {
            DatagramSocket socket = new DatagramSocket(port);
            byte[] buffer = new byte[1024];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                GossipMsg message = GossipMsg.fromJson(new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8));
                msgHandle(message);
                
            }
            // socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void msgHandle(GossipMsg message) {
        // Construct the unique identifier for the message
        String msgKey = message.getSenderId() + "_" + message.getMsgType() + "_" + message.getMsgContent() + "_" + message.getTimestamp();
        // Check if the message has already been received
        Integer count = msgCache.getOrDefault(msgKey, 0);
        if (count >= 1) {
            // Discard if received for the second time or more
            return;
        }
        // First time received, process and count
        msgCache.put(msgKey, count + 1);
        // Control cache size to prevent memory leak
        if (msgCache.size() > CACHE_SIZE_LIMIT) {
            // Simple cleanup strategy: clear all (can be optimized to LRU, etc.)
            msgCache.clear();
        }
        if (message.getMsgType().equals(GossipMsg.Type.HOSTDOWN.toString())) {
            // Handle gossip message: remove host from finger table
            try {
                int hash = Integer.parseInt(message.getMsgContent());
                fingerTable.finger.remove(hash);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (message.getMsgType().equals(GossipMsg.Type.HOSTUP.toString())) {
            // Handle gossip message: add or update host in finger table
            try {
                String[] parts = message.getMsgContent().split("=");
                int hash = Integer.parseInt(parts[0]);
                String address = parts[1];
                fingerTable.addEntry(hash, address);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (message.getMsgType().equals(GossipMsg.Type.HOSTREMOVE.toString())) {
            // Handle gossip message: remove host from finger table
            try {
                int hash = Integer.parseInt(message.getMsgContent());
                fingerTable.finger.remove(hash);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (message.getMsgType().equals(GossipMsg.Type.HOSTADD.toString())) {
            // Handle gossip message: add host to finger table
            try {
                String[] parts = message.getMsgContent().split("=");
                int hash = Integer.parseInt(parts[0]);
                String address = parts[1];
                fingerTable.addEntry(hash, address);
                // After adding, send local finger table to the new node
                // Find the new node in neighbors by address
                GossipNode newNode = null;
                for (GossipNode neighbor : fingerTable.finger.values().stream().map(addr -> {
                    GossipNode node = new GossipNode();
                    node.setIp(addr.split(":")[0].replace("http://", ""));
                    node.setPort(Integer.parseInt(addr.split(":")[1]));
                    node.setStatus("UP");
                    return node;
                }).toList()) {
                    if (neighbor.getIp().equals(address.split(":")[0].replace("http://", "")) && neighbor.getPort() == Integer.parseInt(address.split(":")[1])) {
                        newNode = neighbor;
                        break;
                    }
                }
                if (newNode != null) {
                    // Serialize finger table as string: hash1=addr1,hash2=addr2,...
                    StringBuilder sb = new StringBuilder();
                    for (var entry : fingerTable.finger.entrySet()) {
                        sb.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
                    }
                    if (sb.length() > 0) sb.setLength(sb.length() - 1);
                    sendTableUpdateGossip(newNode, message.getSenderId(), sb.toString(), List.of(newNode), 1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (message.getMsgType().equals(GossipMsg.Type.TABLEUPDATE.toString())) {
            // Handle TABLEUPDATE message: update local finger table from received content
            try {
                String content = message.getMsgContent();
                if (content != null && !content.isEmpty()) {
                    String[] entries = content.split(",");
                    for (String entry : entries) {
                        String[] kv = entry.split("=");
                        if (kv.length == 2) {
                            int hash = Integer.parseInt(kv[0]);
                            String address = kv[1];
                            fingerTable.addEntry(hash, address);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Send HOSTUP gossip message
    public void sendHostUpGossip(GossipNode self, int hash, String address, int numberOfNodes) {
        GossipMsg msg = new GossipMsg();
        msg.setMsgType(GossipMsg.Type.HOSTUP.toString());
        msg.setMsgContent(hash + "=" + address);
        msg.setSenderId(self.getId());
        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
        sendMessage(msg, self.getNeighbors(), numberOfNodes);
    }

    // Send HOSTDOWN gossip message
    public void sendHostDownGossip(GossipNode self, int hash, int numberOfNodes) {
        GossipMsg msg = new GossipMsg();
        msg.setMsgType(GossipMsg.Type.HOSTDOWN.toString());
        msg.setMsgContent(String.valueOf(hash));
        msg.setSenderId(self.getId());
        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
        sendMessage(msg, self.getNeighbors(), numberOfNodes);
    }

    // Send HOSTADD gossip message
    public void sendHostAddGossip(GossipNode self, int hash, String address, int numberOfNodes) {
        GossipMsg msg = new GossipMsg();
        msg.setMsgType(GossipMsg.Type.HOSTADD.toString());
        msg.setMsgContent(hash + "=" + address);
        msg.setSenderId(self.getId());
        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
        sendMessage(msg, self.getNeighbors(), numberOfNodes);
    }

    // Send HOSTREMOVE gossip message
    public void sendHostRemoveGossip(GossipNode self, int hash, int numberOfNodes) {
        GossipMsg msg = new GossipMsg();
        msg.setMsgType(GossipMsg.Type.HOSTREMOVE.toString());
        msg.setMsgContent(String.valueOf(hash));
        msg.setSenderId(self.getId());
        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
        sendMessage(msg, self.getNeighbors(), numberOfNodes);
    }

    // Send TABLEUPDATE gossip message to a specific node
    public void sendTableUpdateGossip(GossipNode self, String receiverId, String tableContent, List<GossipNode> neighbors, int numberOfNodes) {
        GossipMsg msg = new GossipMsg();
        msg.setMsgType(GossipMsg.Type.TABLEUPDATE.toString());
        msg.setMsgContent(tableContent);
        msg.setSenderId(self.getId());
        msg.setReceiverId(receiverId);
        msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
        sendMessage(msg, neighbors, numberOfNodes);
    }
}

    

