package org.distributed.shardingjh.gossip;

import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
    
    // Sender identifier for duplicate detection
    private String senderId;
    
    // Timestamp for duplicate detection
    private String timestamp;

    // Gossip message type
    public enum Type {
        HOST_DOWN,
        HOST_ADD,
        NODE_JOIN,        // Node join discovery request
        HASH_PROPOSAL,    // Hash proposal phase
        HASH_PROPOSAL_ACK,// Hash proposal acknowledgment reply phase
        HASH_CONFIRMATION // Hash confirmation phase
    }
}
