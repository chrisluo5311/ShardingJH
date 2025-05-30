package org.distributed.shardingjh.gossip;

import com.google.gson.Gson;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

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
                    GossipMsg gossipMsg = new Gson().fromJson(received, GossipMsg.class);
                    gossipService.msgHandle(gossipMsg);
                }
            } catch (Exception e) {
                log.error("[GossipReceiver] Failed to receive gossip message: {}", e.getMessage());
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
