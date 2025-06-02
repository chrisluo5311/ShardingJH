package org.distributed.shardingjh.gossip;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class GossipSender {

    public void sendGossip(GossipMsg gossipMsg, String destIp, int destPort) {
        try (DatagramSocket socket = new DatagramSocket()) {
            // Validate input parameters
            if (gossipMsg == null || destIp == null || destIp.trim().isEmpty()) {
                log.error("[GossipSender] Invalid parameters: gossipMsg={}, destIp={}", gossipMsg, destIp);
                return;
            }
            
            String jsonData = new Gson().toJson(gossipMsg);
            
            // Add validation for JSON data
            if (jsonData == null || jsonData.trim().isEmpty()) {
                log.error("[GossipSender] Failed to serialize gossip message: {}", gossipMsg);
                return;
            }
            
            byte[] buf = jsonData.getBytes();
            
            // Log the actual data being sent for debugging
            log.debug("[GossipSender] Sending gossip data: {} bytes to {}:{}", buf.length, destIp, destPort);
            log.trace("[GossipSender] Gossip content: {}", jsonData);
            
            DatagramPacket packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIp), destPort);
            socket.send(packet);
            log.info("[GossipSender] Gossip sent to {}:{}", destIp, destPort);
        } catch (Exception e) {
            log.error("[GossipSender] Failed to send gossip message to {}:{}: {}", destIp, destPort, e.getMessage());
        }
    }
}
