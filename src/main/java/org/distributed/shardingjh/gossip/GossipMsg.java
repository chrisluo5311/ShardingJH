package org.distributed.shardingjh.gossip;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Component
public class GossipMsg {

    // Use enum directly
    private Type msgType;

    // New node example: 64=http://18.222.111.89:8081
    // Host down example: 64
    private String msgContent;

    // Gossip message type
    public enum Type {
        HOST_DOWN,
        HOST_ADD,
    }
}
