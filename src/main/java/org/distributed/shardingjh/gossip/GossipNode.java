package org.distributed.shardingjh.gossip;

import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
public class GossipNode {
    private String id;
    private String ip;
    private int port;
    private String status;
    private List<GossipNode> neighbors = new ArrayList<>();
}
