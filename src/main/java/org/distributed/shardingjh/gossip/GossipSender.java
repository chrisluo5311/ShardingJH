package org.distributed.shardingjh.gossip;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

@Slf4j
@Service
public class GossipSender {

    public void sendGossip(GossipMsg gossipMsg, String destIp, int destPort) {
        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] buf = new Gson().toJson(gossipMsg).getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIp), destPort);
            socket.send(packet);
            log.info("[GossipSender] Gossip sent to {}:{}", destIp, destPort);
        } catch (Exception e) {
            log.error("[GossipSender] Failed to send gossip message: {}", e.getMessage());
        }
    }
}
