package org.distributed.shardingjh.gossip;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class GossipService {
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
            // 这里可以加上消息处理逻辑
        }
        // socket.close(); // 通常不会到这里，除非跳出循环
    } catch (Exception e) {
        e.printStackTrace();
    }
}

public void msgHandle(GossipMsg message) {
    if (message.getMsgType() == GossipMsg.Type.HOSTDOWN.toString()) {
        // 处理 gossip 消息
    } else if (message.getMsgType() == GossipMsg.Type.HOSTUP.toString()) {
        // 处理 gossip 消息
    } else if (message.getMsgType() == GossipMsg.Type.HOSTREMOVE.toString()) {
        // 处理 gossip 消息
    } else if (message.getMsgType() == GossipMsg.Type.HOSTADD.toString()) {
        // 处理 gossip 消息
    }
}

}

    

