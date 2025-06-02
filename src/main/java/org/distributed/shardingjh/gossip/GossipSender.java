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
        sendGossipWithRetries(gossipMsg, destIp, destPort, 1); // Default single attempt
    }
    
    public void sendGossipWithRetries(GossipMsg gossipMsg, String destIp, int destPort, int maxRetries) {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
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
                if (attempt == 1) {
                    log.debug("[GossipSender] Sending gossip data: {} bytes to {}:{}", buf.length, destIp, destPort);
                    log.trace("[GossipSender] Gossip content: {}", jsonData);
                }
                
                DatagramPacket packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIp), destPort);
                socket.send(packet);
                
                if (attempt == 1) {
                    log.info("[GossipSender] Gossip sent to {}:{}", destIp, destPort);
                } else {
                    log.info("[GossipSender] Gossip sent to {}:{} (attempt {}/{})", destIp, destPort, attempt, maxRetries);
                }
                return; // Success, exit retry loop
                
            } catch (Exception e) {
                lastException = e;
                if (attempt < maxRetries) {
                    log.warn("[GossipSender] Attempt {}/{} failed to send gossip to {}:{}: {}, retrying...", 
                            attempt, maxRetries, destIp, destPort, e.getMessage());
                    try {
                        Thread.sleep(50); // Small delay between retries
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    log.error("[GossipSender] All {} attempts failed to send gossip message to {}:{}: {}", 
                             maxRetries, destIp, destPort, e.getMessage());
                }
            }
        }
        
        // If we get here, all retries failed
        if (lastException != null) {
            log.error("[GossipSender] Final failure after {} attempts to send gossip to {}:{}", 
                     maxRetries, destIp, destPort, lastException);
        }
    }
}
