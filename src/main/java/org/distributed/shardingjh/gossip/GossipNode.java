package org.distributed.shardingjh.gossip;

import java.util.ArrayList;
import java.util.List;

public class GossipNode {
    private String id;
    private String ip;
    private int port;
    private String status;
    private List<GossipNode> neighbors = new ArrayList<>();
    public List<GossipNode> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(List<GossipNode> neighbors) {
        this.neighbors = neighbors;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
