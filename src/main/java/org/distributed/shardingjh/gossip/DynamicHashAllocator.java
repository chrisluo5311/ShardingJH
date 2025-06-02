package org.distributed.shardingjh.gossip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.distributed.shardingjh.common.constant.ShardConst;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

/**
 * Dynamic hash allocator, implementing distributed hash allocation based on gossip protocol
 */
@Slf4j
@Component
public class DynamicHashAllocator {
    
    @Resource
    private FingerTable fingerTable;
    
    @Resource
    @Lazy
    private GossipService gossipService;
    
    @Resource
    private BootstrapService bootstrapService;
    
    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;
    
    // Cache for ongoing hash allocation requests
    private final ConcurrentHashMap<String, NodeJoinRequest> pendingRequests = new ConcurrentHashMap<>();
    
    // Hash reservation table, recording hash values being applied for
    private final ConcurrentHashMap<Integer, NodeJoinRequest> hashReservations = new ConcurrentHashMap<>();
    
    // Proposal acknowledgment collector, key is requestId, value is list of received confirmations
    private final ConcurrentHashMap<String, Set<String>> proposalAcknowledgments = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    private static final long RESERVATION_TIMEOUT_MS = 10000; // 10 second timeout
    private static final long CONFIRMATION_TIMEOUT_MS = 5000;  // 5 second confirmation timeout
    
    /**
     * Start dynamic hash allocation process
     * @return Allocated hash value
     */
    public Integer allocateHashForCurrentNode() throws InterruptedException {
        log.info("[DynamicHashAllocator] Starting dynamic hash allocation for node {}", CURRENT_NODE_URL);
        
        // Phase 1: Discover current network state
        Map<Integer, String> networkFingerTable = discoverNetworkState();
        
        // Phase 2: Find optimal hash position
        Integer proposedHash = findOptimalHashPosition(networkFingerTable, CURRENT_NODE_URL);
        
        // Phase 3: Two-phase commit to apply hash
        return requestHashAllocation(proposedHash);
    }
    
    /**
     * Phase 1: Discover current network state via gossip
     */
    private Map<Integer, String> discoverNetworkState() {
        log.info("[DynamicHashAllocator] Phase 1: Discovering network state via gossip");
        
        // Register current node as bootstrapping
        bootstrapService.registerBootstrapNode();
        
        // Send discovery request to all known nodes (including bootstrap nodes)
        NodeJoinRequest discoveryRequest = NodeJoinRequest.builder()
                .nodeUrl(CURRENT_NODE_URL)
                .phase(NodeJoinRequest.Phase.DISCOVERY)
                .timestamp(System.currentTimeMillis())
                .build();
        
        // Send discovery request via gossip
        GossipMsg gossipMsg = GossipMsg.builder()
                .msgType(GossipMsg.Type.NODE_JOIN)
                .msgContent(serializeJoinRequest(discoveryRequest))
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();
        
        // Use BootstrapService to get all known nodes (finger table + configured nodes + bootstrap nodes)
        List<String> allKnownNodes = bootstrapService.getAllKnownNodes();
        log.info("[DynamicHashAllocator] Sending discovery to all known nodes: {}", allKnownNodes);
        
        if (!allKnownNodes.isEmpty()) {
            gossipService.randomSendGossip(gossipMsg, allKnownNodes);
        } else {
            log.warn("[DynamicHashAllocator] No known nodes found for discovery, continuing with current finger table");
        }
        
        // Wait for response collection, then merge finger table information
        try {
            Thread.sleep(2000); // Wait for gossip propagation
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return new TreeMap<>(fingerTable.finger);
    }
    
    /**
     * Phase 2: Find optimal hash position
     * Strategy: Find the largest gap between existing nodes and then select the midpoint of the gap
     */
    private Integer findOptimalHashPosition(Map<Integer, String> currentTable, String nodeUrl) {
        log.info("[DynamicHashAllocator] Phase 2: Finding optimal hash position");
        log.info("[DynamicHashAllocator] Current finger table: {}", currentTable);
        
        if (currentTable.isEmpty()) {
            // First node in the network
            int firstNodeHash = Math.abs(nodeUrl.hashCode()) % ShardConst.FINGER_MAX_RANGE;
            log.info("[DynamicHashAllocator] First node in network, using hash: {}", firstNodeHash);
            return firstNodeHash;
        }
        
        List<Integer> sortedHashes = new ArrayList<>(currentTable.keySet());
        Collections.sort(sortedHashes);
        
        int maxGap = 0;
        int bestPosition = 0;
        
        // Check gaps between adjacent nodes
        for (int i = 0; i < sortedHashes.size(); i++) {
            int current = sortedHashes.get(i);
            int next = (i + 1 < sortedHashes.size()) ? 
                       sortedHashes.get(i + 1) : 
                       (sortedHashes.get(0) + ShardConst.FINGER_MAX_RANGE);
            
            int gap;
            if (i + 1 < sortedHashes.size()) {
                // Normal gap between adjacent nodes
                gap = next - current;
            } else {
                // Wrap-around gap from last node to first node
                gap = (ShardConst.FINGER_MAX_RANGE - current) + sortedHashes.get(0);
            }
            
            if (gap > maxGap && gap > 1) { // Ensure gap is greater than 1
                maxGap = gap;
                // Select midpoint of the gap
                if (i + 1 < sortedHashes.size()) {
                    bestPosition = current + gap / 2;
                } else {
                    // Handle wrap-around case
                    bestPosition = (current + gap / 2) % ShardConst.FINGER_MAX_RANGE;
                }
            }
        }
        
        // If no suitable gap is found, use linear probing
        if (maxGap <= 1) {
            bestPosition = findAvailableHashLinear(currentTable);
        }
        
        log.info("[DynamicHashAllocator] Found optimal position: {} (gap size: {})", bestPosition, maxGap);
        return bestPosition;
    }
    
    /**
     * Linear probing to find available hash position
     */
    private Integer findAvailableHashLinear(Map<Integer, String> currentTable) {
        return findAvailableHashLinearExcluding(currentTable, new HashSet<>());
    }
    
    /**
     * Linear probing to find available hash position, excluding specified hashes
     * @param currentTable Current finger table
     * @param excludedHashes Hashes to exclude from consideration
     * @return Available hash or null if none found
     */
    private Integer findAvailableHashLinearExcluding(Map<Integer, String> currentTable, Set<Integer> excludedHashes) {
        // Also exclude hashes from configuration to avoid conflicts
        Set<Integer> allExcluded = new HashSet<>(excludedHashes);
        allExcluded.addAll(currentTable.keySet());
        
        // Parse finger.entries configuration to exclude configured hashes
        try {
            for (String entry : bootstrapService.getConfiguredEntries()) {
                String[] parts = entry.split("=");
                if (parts.length == 2) {
                    Integer configHash = Integer.parseInt(parts[0]);
                    allExcluded.add(configHash);
                    log.debug("[findAvailableHashLinearExcluding] Excluding configured hash: {}", configHash);
                }
            }
        } catch (Exception e) {
            log.warn("[findAvailableHashLinearExcluding] Failed to parse configuration: {}", e.getMessage());
        }
        
        int start = Math.abs(CURRENT_NODE_URL.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        for (int i = 0; i < ShardConst.FINGER_MAX_RANGE; i++) {
            int candidate = (start + i) % ShardConst.FINGER_MAX_RANGE;
            if (!allExcluded.contains(candidate)) {
                log.info("[findAvailableHashLinearExcluding] Found available hash {} after {} attempts (excluded: {})", 
                        candidate, i, allExcluded.size());
                return candidate;
            }
        }
        log.error("[findAvailableHashLinearExcluding] No available hash found after {} attempts, excluded {} hashes", 
                ShardConst.FINGER_MAX_RANGE, allExcluded.size());
        return null;
    }
    
    /**
     * Phase 3: Two-phase commit to apply hash (improved version - wait for majority confirmation)
     */
    private Integer requestHashAllocation(Integer proposedHash) throws InterruptedException {
        log.info("[DynamicHashAllocator] Phase 3: Two-phase commit to apply hash: {}", proposedHash);
        
        // Check if local conflict exists
        if (hashReservations.containsKey(proposedHash)) {
            NodeJoinRequest conflictingRequest = hashReservations.get(proposedHash);
            if (shouldYieldToConflictingRequest(conflictingRequest)) {
                log.info("[DynamicHashAllocator] Yielding to higher priority request for hash: {}", proposedHash);
                return findAvailableHashLinear(fingerTable.finger);
            }
        }
        
        // 🔒 Sub-phase 3.1: PROPOSAL (Proposal phase)
        NodeJoinRequest proposalRequest = NodeJoinRequest.builder()
                .nodeUrl(CURRENT_NODE_URL)
                .phase(NodeJoinRequest.Phase.PROPOSAL)
                .proposedHash(proposedHash)
                .timestamp(System.currentTimeMillis())
                .build();
        
        String requestId = proposalRequest.generateConflictResolver();
        pendingRequests.put(requestId, proposalRequest);
        hashReservations.put(proposedHash, proposalRequest);
        
        // Initialize acknowledgment collector
        proposalAcknowledgments.put(requestId, ConcurrentHashMap.newKeySet());
        
        // Send proposal gossip
        GossipMsg proposalGossip = GossipMsg.builder()
                .msgType(GossipMsg.Type.HASH_PROPOSAL)
                .msgContent(serializeJoinRequest(proposalRequest))
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();
        
        // Use all known nodes for proposal sending
        List<String> allNodes = bootstrapService.getAllKnownNodes();
        log.info("[DynamicHashAllocator] Sending proposal to all known nodes: {}", allNodes);
        
        if (!allNodes.isEmpty()) {
            gossipService.randomSendGossip(proposalGossip, allNodes);
        } else {
            log.warn("[DynamicHashAllocator] No nodes found for proposal, using finger table only");
            allNodes = new ArrayList<>(fingerTable.finger.values());
            gossipService.randomSendGossip(proposalGossip, allNodes);
        }
        
        // ⏰ Wait for acknowledgment collection period
        log.info("[DynamicHashAllocator] Waiting for network node to confirm proposal...");
        log.info("[DynamicHashAllocator] Request ID: {}, waiting for confirmations from {} nodes", requestId, allNodes.size());
        Thread.sleep(RESERVATION_TIMEOUT_MS); // 10 seconds
        
        // 📊 Check acknowledgment result
        Set<String> confirmations = proposalAcknowledgments.get(requestId);
        int totalNodes = allNodes.size();
        int confirmationCount = confirmations != null ? confirmations.size() : 0;
        
        log.info("[DynamicHashAllocator] After waiting period - RequestID: {}, Confirmations received: {}, Total nodes: {}", 
                requestId, confirmations != null ? confirmations : "null", totalNodes);
        
        // Handle single-node network case
        int majorityThreshold;
        if (totalNodes == 0) {
            // First node in network - no confirmations needed
            majorityThreshold = 0;
            log.info("[DynamicHashAllocator] First node in network, no confirmation needed");
        } else {
            majorityThreshold = (totalNodes / 2) + 1; // Majority for multi-node network
        }
        
        log.info("[DynamicHashAllocator] Confirmation statistics: {}/{} nodes confirmed, need: {}", 
                confirmationCount, totalNodes, majorityThreshold);
        
        // Check if sufficient confirmation is obtained
        if (confirmationCount < majorityThreshold) {
            log.warn("[DynamicHashAllocator] ❌ INSUFFICIENT CONFIRMATION ({}/{}), STARTING RETRY PROCESS", 
                    confirmationCount, majorityThreshold);
            cleanup(requestId, proposedHash);
            
            // Remove the failed hash from consideration to avoid retry loops
            Set<Integer> excludedHashes = new HashSet<>();
            excludedHashes.add(proposedHash);
            Integer retryHash = findAvailableHashLinearExcluding(fingerTable.finger, excludedHashes);
            
            if (retryHash != null) {
                log.info("[DynamicHashAllocator] 🔄 RETRYING HASH ALLOCATION with different hash: {} (excluded: {})", 
                        retryHash, proposedHash);
                return requestHashAllocation(retryHash);
            } else {
                log.error("[DynamicHashAllocator] ❌ RETRY FAILED: Unable to find alternative hash after confirmation failure");
                throw new RuntimeException("Unable to find alternative hash after confirmation failure");
            }
        }
        
        // Again check conflict (Possible conflict at higher priority request during waiting period)
        NodeJoinRequest currentReservation = hashReservations.get(proposedHash);
        if (currentReservation != null && !currentReservation.getNodeUrl().equals(CURRENT_NODE_URL)) {
            log.warn("[DynamicHashAllocator] Detected priority conflict during waiting period, yield to: {}", 
                    currentReservation.getNodeUrl());
            cleanup(requestId, proposedHash);
            
            // Use excluding logic to avoid selecting the same conflicted hash
            Set<Integer> excludedHashes = new HashSet<>();
            excludedHashes.add(proposedHash);
            Integer fallbackHash = findAvailableHashLinearExcluding(fingerTable.finger, excludedHashes);
            
            if (fallbackHash != null) {
                log.info("[DynamicHashAllocator] Using fallback hash {} after conflict", fallbackHash);
                return requestHashAllocation(fallbackHash);
            } else {
                throw new RuntimeException("Unable to find fallback hash after priority conflict");
            }
        }
        
        // ✅ Sub-phase 3.2: CONFIRMATION (Confirmation phase)
        log.info("[DynamicHashAllocator] 🎉 Obtained enough confirmation, enter CONFIRMATION phase");
        proposalRequest.setPhase(NodeJoinRequest.Phase.CONFIRMATION);
        
        GossipMsg confirmationGossip = GossipMsg.builder()
                .msgType(GossipMsg.Type.HASH_CONFIRMATION)
                .msgContent(serializeJoinRequest(proposalRequest))
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();
        
        gossipService.randomSendGossip(confirmationGossip, allNodes);
        
        // Clean up temporary data
        cleanup(requestId, proposedHash);
        
        // Add to local finger table immediately
        fingerTable.finger.put(proposedHash, CURRENT_NODE_URL);
        
        // Unregister from bootstrap service after successful allocation
        bootstrapService.unregisterBootstrapNode();
        
        log.info("[DynamicHashAllocator] 🚀 Successfully allocated hash: {} to node: {}", proposedHash, CURRENT_NODE_URL);
        return proposedHash;
    }
    
    /**
     * Clean up request-related temporary data
     */
    private void cleanup(String requestId, Integer hash) {
        pendingRequests.remove(requestId);
        proposalAcknowledgments.remove(requestId);
        // Note: Do not clean up hashReservations, as other methods may be using it
    }
    
    /**
     * Handle received hash allocation-related gossip messages
     */
    public void handleHashAllocationGossip(GossipMsg message) {
        NodeJoinRequest request = deserializeJoinRequest(message.getMsgContent());
        
        switch (request.getPhase()) {
            case DISCOVERY:
                handleDiscoveryRequest(request);
                break;
            case PROPOSAL:
                handleHashProposal(request);
                break;
            case PROPOSAL_ACK:
                handleProposalAcknowledgment(request);
                break;
            case CONFIRMATION:
                handleHashConfirmation(request);
                break;
            default:
                log.warn("[DynamicHashAllocator] Unknown phase: {}", request.getPhase());
        }
    }
    
    /**
     * Handle discovery request - send current finger table back to requesting node
     */
    private void handleDiscoveryRequest(NodeJoinRequest request) {
        String requesterNode = request.getNodeUrl();
        log.info("[DynamicHashAllocator] 📡 Received discovery request from: {}", requesterNode);
        
        // Send current finger table back to help the new node
        if (!fingerTable.finger.isEmpty()) {
            GossipMsg fingertableSnapshot = GossipMsg.builder()
                    .msgType(GossipMsg.Type.HOST_ADD)
                    .msgContent(fingerTable.finger.toString())
                    .senderId(CURRENT_NODE_URL)
                    .timestamp(String.valueOf(System.currentTimeMillis()))
                    .build();
            
            log.info("[DynamicHashAllocator] 📤 Sending finger table snapshot to {}: {}", 
                    requesterNode, fingerTable.finger);
            gossipService.randomSendGossip(fingertableSnapshot, List.of(requesterNode));
        } else {
            log.debug("[DynamicHashAllocator] No finger table to share with discovery request from: {}", requesterNode);
        }
    }
    
    /**
     * Handle hash proposal (improved version - send confirmation reply)
     */
    private void handleHashProposal(NodeJoinRequest request) {
        Integer hash = request.getProposedHash();
        String requesterNode = request.getNodeUrl();
        log.info("[DynamicHashAllocator] 📥 Received hash proposal: {} from: {}", hash, requesterNode);
        
        boolean accepted = false;
        String reason = "";
        
        NodeJoinRequest existingRequest = hashReservations.get(hash);
        if (existingRequest == null) {
            // ✅ No conflict, accept reservation
            hashReservations.put(hash, request);
            accepted = true;
            reason = "No conflict, accept proposal";
            log.info("[DynamicHashAllocator] ✅ Accepted hash proposal: {}", hash);
        } else if (existingRequest.getNodeUrl().equals(requesterNode)) {
            // ✅ Same node request, accept
            accepted = true;
            reason = "Same node request, accept";
            log.info("[DynamicHashAllocator] ✅ Repeat request, accept hash proposal: {}", hash);
        } else {
            // ⚔️ Conflict, compare priority
            if (shouldYieldToConflictingRequest(request)) {
                hashReservations.put(hash, request);  // Yield to higher priority
                accepted = true;
                reason = "Yield to higher priority request";
                log.info("[DynamicHashAllocator] 🔄 Conflict resolution: Yield to higher priority request");
            } else {
                accepted = false;
                reason = "Conflict with higher priority request";
                log.info("[DynamicHashAllocator] 🛡️ Conflict resolution: Reject, keep current reservation");
            }
        }
        
        // 📤 Send confirmation reply
        sendProposalAcknowledgment(request, accepted, reason);
    }
    
    /**
     * Send proposal acknowledgment reply
     */
    private void sendProposalAcknowledgment(NodeJoinRequest originalRequest, boolean accepted, String reason) {
        NodeJoinRequest ackRequest = NodeJoinRequest.builder()
                .nodeUrl(originalRequest.getNodeUrl())      // Original request node
                .phase(NodeJoinRequest.Phase.PROPOSAL_ACK)  // Confirmation phase
                .proposedHash(originalRequest.getProposedHash())
                .timestamp(originalRequest.getTimestamp())  // Keep original timestamp
                .accepted(accepted)                         // Whether accepted
                .respondingNode(CURRENT_NODE_URL)           // Reply node
                .conflictResolver(originalRequest.generateConflictResolver()) // Keep original conflict resolver
                .build();
        
        GossipMsg ackGossip = GossipMsg.builder()
                .msgType(GossipMsg.Type.HASH_PROPOSAL_ACK)
                .msgContent(serializeJoinRequest(ackRequest))
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();
        
        // Send directly to request node
        List<String> targetNode = List.of(originalRequest.getNodeUrl());
        gossipService.randomSendGossip(ackGossip, targetNode);
        
        log.info("[DynamicHashAllocator] 📤 Sent confirmation reply to {}: {} ({})", 
                originalRequest.getNodeUrl(), accepted ? "Accepted" : "Rejected", reason);
    }
    
    /**
     * Handle proposal acknowledgment reply
     */
    private void handleProposalAcknowledgment(NodeJoinRequest request) {
        String requesterNode = request.getNodeUrl();
        String respondingNode = request.getRespondingNode();
        Boolean accepted = request.getAccepted();
        
        log.info("[DynamicHashAllocator] 📨 Received confirmation reply from {}: {} (for hash {} of node {})", 
                respondingNode, accepted ? "Accepted" : "Rejected", request.getProposedHash(), requesterNode);
        
        // Only handle confirmation replies addressed to current node
        if (!requesterNode.equals(CURRENT_NODE_URL)) {
            log.debug("[DynamicHashAllocator] Confirmation reply not addressed to current node, ignore");
            return;
        }
        
        String requestId = request.generateConflictResolver();
        log.debug("[DynamicHashAllocator] Looking for request ID: {}", requestId);
        
        Set<String> confirmations = proposalAcknowledgments.get(requestId);
        
        if (confirmations == null) {
            log.warn("[DynamicHashAllocator] No pending request found for requestId: {}, available requests: {}", 
                    requestId, proposalAcknowledgments.keySet());
            return;
        }
        
        if (accepted != null && accepted) {
            boolean added = confirmations.add(respondingNode);
            if (added) {
                log.info("[DynamicHashAllocator] ✅ Added confirmation from {}, current confirmation count: {}/{}", 
                        respondingNode, confirmations.size(), confirmations);
            } else {
                log.debug("[DynamicHashAllocator] Duplicate confirmation from {}, ignoring", respondingNode);
            }
        } else if (accepted != null && !accepted) {
            log.warn("[DynamicHashAllocator] ❌ Received rejection from {} for hash {}", respondingNode, request.getProposedHash());
        } else {
            log.warn("[DynamicHashAllocator] Received invalid confirmation reply from {} - accepted field is null", respondingNode);
        }
    }
    
    /**
     * Handle hash confirmation
     */
    private void handleHashConfirmation(NodeJoinRequest request) {
        Integer hash = request.getProposedHash();
        String nodeUrl = request.getNodeUrl();
        
        log.info("[DynamicHashAllocator] Received hash confirmation for hash: {} from node: {}", hash, nodeUrl);
        
        // Check for hash conflicts before adding
        String existingNode = fingerTable.finger.get(hash);
        if (existingNode != null && !existingNode.equals(nodeUrl)) {
            log.error("[DynamicHashAllocator] HASH CONFLICT DETECTED! Hash {} already occupied by {} but requested by {}", 
                     hash, existingNode, nodeUrl);
            
            // Send rejection message back to the conflicting node
            GossipMsg rejectionMsg = GossipMsg.builder()
                    .msgType(GossipMsg.Type.HOST_DOWN)
                    .msgContent("HASH_CONFLICT:" + hash + ":" + nodeUrl)
                    .senderId(CURRENT_NODE_URL)
                    .timestamp(String.valueOf(System.currentTimeMillis()))
                    .build();
            
            gossipService.randomSendGossip(rejectionMsg, List.of(nodeUrl));
            return; // Do not add the conflicting entry
        }
        
        // Official addition to finger table
        fingerTable.finger.put(hash, nodeUrl);
        hashReservations.remove(hash);
        
        log.info("[DynamicHashAllocator] Added node to finger table: {} -> {}", hash, nodeUrl);
    }
    
    /**
     * Determine whether to yield to conflicting request
     * Based on priority: Earlier request timestamp has higher priority
     */
    private boolean shouldYieldToConflictingRequest(NodeJoinRequest conflictingRequest) {
        NodeJoinRequest currentRequest = pendingRequests.get(conflictingRequest.generateConflictResolver());
        if (currentRequest == null) {
            return true; // No current request, yield
        }
        
        return conflictingRequest.calculatePriority() < currentRequest.calculatePriority();
    }
    
    /**
     * Serialize join request (Updated version - Support new fields)
     */
    private String serializeJoinRequest(NodeJoinRequest request) {
        return String.format("%s|%s|%s|%d|%d|%s|%s|%s", 
                request.getNodeUrl(),
                request.getPhase().name(),
                request.getProposedHash() != null ? request.getProposedHash().toString() : "null",
                request.getTimestamp(),
                request.calculatePriority(),
                request.getAccepted() != null ? request.getAccepted().toString() : "null",
                request.getRespondingNode() != null ? request.getRespondingNode() : "null",
                request.generateConflictResolver() != null ? request.generateConflictResolver() : "null");
    }
    
    /**
     * Deserialize join request (Updated version - Support new fields)
     */
    private NodeJoinRequest deserializeJoinRequest(String content) {
        String[] parts = content.split("\\|");
        NodeJoinRequest request = NodeJoinRequest.builder()
                .nodeUrl(parts[0])
                .phase(NodeJoinRequest.Phase.valueOf(parts[1]))
                .proposedHash("null".equals(parts[2]) ? null : Integer.parseInt(parts[2]))
                .timestamp(Long.parseLong(parts[3]))
                .priority(Integer.parseInt(parts[4]))
                .accepted(parts.length > 5 && !"null".equals(parts[5]) ? Boolean.parseBoolean(parts[5]) : null)
                .respondingNode(parts.length > 6 && !"null".equals(parts[6]) ? parts[6] : null)
                .conflictResolver(parts.length > 7 && !"null".equals(parts[7]) ? parts[7] : null)
                .build();
        
        return request;
    }
    
    /**
     * Clean up expired reservation
     */
    private void cleanupExpiredReservations() {
        long now = System.currentTimeMillis();
        hashReservations.entrySet().removeIf(entry -> {
            NodeJoinRequest request = entry.getValue();
            boolean expired = (now - request.getTimestamp()) > RESERVATION_TIMEOUT_MS;
            if (expired) {
                log.info("[DynamicHashAllocator] Cleaning up expired reservation for hash: {}", entry.getKey());
            }
            return expired;
        });
    }
} 