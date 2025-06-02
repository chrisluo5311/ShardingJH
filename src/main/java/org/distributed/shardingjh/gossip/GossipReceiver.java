package org.distributed.shardingjh.gossip;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class GossipReceiver implements ApplicationRunner {

    @Value("${gossip.port}")
    private int PORT; // 9000

    @Resource
    GossipService gossipService;

    @Override
    public void run(ApplicationArguments args) {
        Thread t = new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket(PORT)) {
                byte[] buf = new byte[4096];
                System.out.println("GossipReceiver listening on port " + PORT);

                while (true) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);
                    String received = new String(packet.getData(), 0, packet.getLength());
                    log.info("Received: {}", received);
                    
                    try {
                        GossipMsg gossipMsg = new Gson().fromJson(received, GossipMsg.class);
                        
                        // Add special logging for HASH_PROPOSAL_ACK messages
                        if (gossipMsg.getMsgType() == GossipMsg.Type.HASH_PROPOSAL_ACK) {
                            log.info("[GossipReceiver] 🎯 CRITICAL: Received HASH_PROPOSAL_ACK from {}", gossipMsg.getSenderId());
                            log.info("[GossipReceiver] 📦 ACK Content: {}", gossipMsg.getMsgContent());
                        }
                        
                        gossipService.msgHandle(gossipMsg);
                    } catch (Exception e) {
                        log.error("[GossipReceiver] Failed to parse or handle gossip message: {}", e.getMessage());
                        log.error("[GossipReceiver] Problematic message: {}", received);
                    }
                }
            } catch (Exception e) {
                log.error("[GossipReceiver] Failed to receive gossip message: {}", e.getMessage());
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
