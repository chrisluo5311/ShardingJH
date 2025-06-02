package org.distributed.shardingjh.gossip;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Node join request, supporting two-phase hash allocation protocol
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NodeJoinRequest {
    
    public enum Phase {
        DISCOVERY,    // Phase 1: Discovery phase, get current finger table
        PROPOSAL,     // Phase 2: Proposal phase, propose to occupy a hash
        PROPOSAL_ACK, // Phase 2.5: Proposal acknowledgment phase, nodes reply whether to accept proposal
        CONFIRMATION, // Phase 3: Confirmation phase, confirm hash allocation
        REJECTION     // Phase 4: Rejection phase, hash conflict rejected
    }
    
    private String nodeUrl;           // URL of the node requesting to join
    private Phase phase;              // Current phase
    private Integer proposedHash;     // Proposed hash value
    private Long timestamp;           // Request timestamp
    private String fingerTableSnapshot; // finger table snapshot
    private String conflictResolver;  // Conflict resolution identifier
    private Integer priority;         // Priority (based on timestamp and nodeUrl calculation)
    private Boolean accepted;         // Whether to accept proposal (for PROPOSAL_ACK phase)
    private String respondingNode;    // Responding node (for PROPOSAL_ACK phase)
    
    /**
     * Calculate request priority for conflict resolution
     * Priority calculation: smaller timestamp has higher priority, if timestamp is same then by nodeUrl lexicographic order
     */
    public int calculatePriority() {
        if (priority == null) {
            // timestamp has higher weight, nodeUrl as tie-breaker
            priority = (int) (timestamp % 1000000) + nodeUrl.hashCode() % 1000;
        }
        return priority;
    }
    
    /**
     * Generate unique conflict resolution identifier
     */
    public String generateConflictResolver() {
        if (conflictResolver == null) {
            conflictResolver = nodeUrl + "@" + timestamp;
        }
        return conflictResolver;
    }
} 