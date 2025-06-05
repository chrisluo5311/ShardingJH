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
import java.util.concurrent.TimeUnit;

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
    
    @Resource
    private HashAllocationHTTPClient httpClient;
    
    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;
    
    @Value("${finger.entries}")
    private String entries;
    
    // Cache for ongoing hash allocation requests
    private final ConcurrentHashMap<String, NodeJoinRequest> pendingRequests = new ConcurrentHashMap<>();
    
    // Hash reservation table, recording hash values being applied for
    private final ConcurrentHashMap<Integer, NodeJoinRequest> hashReservations = new ConcurrentHashMap<>();
    
    // Proposal acknowledgment collector, key is requestId, value is list of received confirmations
    private final ConcurrentHashMap<String, Set<String>> proposalAcknowledgments = new ConcurrentHashMap<>();
    
    // New data structure to track accepted proposals
    private final ConcurrentHashMap<Integer, String> acceptedProposals = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    // Anti-isolation retry scheduler
    private final ScheduledExecutorService isolationPreventionScheduler = Executors.newScheduledThreadPool(1);
    
    // Whether in temporary single node mode (waiting for other nodes to join)
    private volatile boolean isTemporarySingleNode = false;
    
    private static final long RESERVATION_TIMEOUT_MS = 10000; // 10 second timeout
    private static final long CONFIRMATION_TIMEOUT_MS = 5000;  // 5 second confirmation timeout
    private static final long ISOLATION_PREVENTION_INTERVAL_MS = 15000; // Check every 15 seconds
    private static final long TEMPORARY_SINGLE_NODE_TIMEOUT_MS = 60000; // Consider as true single node after 60 seconds
    
    /**
     * Start dynamic hash allocation process
     * @return Allocated hash value
     */
    public Integer allocateHashForCurrentNode() throws InterruptedException {
        log.info("[DynamicHashAllocator] Starting dynamic hash allocation for node {}", CURRENT_NODE_URL);
        
        // 首先检查当前节点是否已经有hash分配，防止重复分配
        Integer existingHash = findCurrentNodeHash();
        if (existingHash != null) {
            log.info("[DynamicHashAllocator] Node already has hash assignment: {}, skipping allocation to prevent duplicates", existingHash);
            return existingHash;
        }
        
        // Phase 1: Discover current network state
        Map<Integer, String> networkFingerTable = discoverNetworkState();
        
        // Phase 2: Find optimal hash position
        Integer proposedHash = findOptimalHashPosition(networkFingerTable, CURRENT_NODE_URL);
        
        // Phase 3: Two-phase commit to apply hash
        return requestHashAllocation(proposedHash);
    }
    
    /**
     * Find existing hash assignment for current node
     * @return Hash value if found, null otherwise
     */
    private Integer findCurrentNodeHash() {
        for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
            if (entry.getValue().equals(CURRENT_NODE_URL)) {
                log.debug("[DynamicHashAllocator] Found existing hash assignment: {} -> {}", entry.getKey(), entry.getValue());
                return entry.getKey();
            }
        }
        return null;
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
        log.info("[DynamicHashAllocator] Finding optimal hash position for node: {}", nodeUrl);
        log.info("[DynamicHashAllocator] Current network finger table: {}", currentTable);
        
        // Get all reserved hashes from configuration to avoid conflicts
        Set<Integer> reservedHashes = getReservedHashesFromConfig();
        log.info("[DynamicHashAllocator] Reserved hashes from configuration: {}", reservedHashes);
        
        // Combine current table hashes with reserved hashes to get all unavailable positions
        Set<Integer> unavailableHashes = new HashSet<>(currentTable.keySet());
        unavailableHashes.addAll(reservedHashes);
        log.info("[DynamicHashAllocator] All unavailable hashes (current + reserved): {}", unavailableHashes);
        
        if (unavailableHashes.isEmpty()) {
            // Network is empty and no config reservations, choose a reasonable starting position
            int initialHash = 64;
            log.info("[DynamicHashAllocator] Empty network, selecting initial hash: {}", initialHash);
            return initialHash;
        }
        
        // Find the largest gap between consecutive hashes
        List<Integer> sortedHashes = new ArrayList<>(unavailableHashes);
        Collections.sort(sortedHashes);
        
        int maxGap = 0;
        int bestPosition = -1;
        
        // Check gap between each consecutive pair
        for (int i = 0; i < sortedHashes.size() - 1; i++) {
            int currentHash = sortedHashes.get(i);
            int nextHash = sortedHashes.get(i + 1);
            int gap = nextHash - currentHash;
            
            if (gap > maxGap && gap > 1) { // Need at least gap of 2 to fit a new hash
                maxGap = gap;
                bestPosition = currentHash + gap / 2; // Midpoint of the gap
            }
        }
        
        // Check gap after the last hash (wrap-around to 256)
        int lastHash = sortedHashes.get(sortedHashes.size() - 1);
        int wrapAroundGap = (256 - lastHash) + sortedHashes.get(0);
        if (wrapAroundGap > maxGap && wrapAroundGap > 1) {
            maxGap = wrapAroundGap;
            bestPosition = (lastHash + wrapAroundGap / 2) % 256;
        }
        
        // Check gap before the first hash (wrap-around from 0)
        int firstHash = sortedHashes.get(0);
        if (firstHash > 1 && (firstHash > maxGap)) {
            maxGap = firstHash;
            bestPosition = firstHash / 2;
        }
        
        if (bestPosition == -1) {
            log.error("[DynamicHashAllocator] Could not find suitable hash position! All positions may be occupied.");
            log.error("[DynamicHashAllocator] Unavailable hashes: {}", unavailableHashes);
            throw new RuntimeException("Cannot find available hash position - network may be full or misconfigured");
        }
        
        log.info("[DynamicHashAllocator] Selected optimal hash position: {} (gap size: {})", bestPosition, maxGap);
        return bestPosition;
    }
    
    /**
     * Get all reserved hashes from finger.entries configuration
     * This prevents dynamic allocation from conflicting with configured nodes
     */
    private Set<Integer> getReservedHashesFromConfig() {
        Set<Integer> reservedHashes = new HashSet<>();
        
        try {
            if (entries != null && !entries.trim().isEmpty()) {
                String[] configEntries = entries.split(",");
                for (String entry : configEntries) {
                    String[] parts = entry.trim().split("=");
                    if (parts.length == 2) {
                        try {
                            int hash = Integer.parseInt(parts[0].trim());
                            reservedHashes.add(hash);
                            log.debug("[DynamicHashAllocator] Found reserved hash from config: {} -> {}", hash, parts[1].trim());
                        } catch (NumberFormatException e) {
                            log.warn("[DynamicHashAllocator] Invalid hash format in config entry: {}", entry);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("[DynamicHashAllocator] Error parsing finger.entries configuration: {}", e.getMessage());
        }
        
        log.info("[DynamicHashAllocator] Total reserved hashes from configuration: {}", reservedHashes.size());
        return reservedHashes;
    }
    
    /**
     * Phase 3: Two-phase commit to apply hash (improved version - wait for majority confirmation)
     */
    private Integer requestHashAllocation(Integer proposedHash) throws InterruptedException {
        log.info("[DynamicHashAllocator] Phase 3: Two-phase commit to apply hash: {}", proposedHash);
        
        // Get all known nodes first to determine if we're truly alone
        List<String> allNodes = bootstrapService.getAllKnownNodes();
        
        // Check if this is truly a single-node network (no other known nodes)
        if (allNodes.isEmpty()) {
            log.info("[DynamicHashAllocator] 🤔 No other nodes detected. Checking if this is temporary isolation...");
            
            // First time detecting no other nodes, start anti-isolation mechanism
            if (!isTemporarySingleNode) {
                log.info("[DynamicHashAllocator] 🚨 Starting isolation prevention mechanism");
                isTemporarySingleNode = true;
                startIsolationPreventionScheduler();
                
                // Wait for a while to give other nodes a chance to start
                log.info("[DynamicHashAllocator] ⏳ Waiting {}ms for other nodes to come online...", TEMPORARY_SINGLE_NODE_TIMEOUT_MS);
                Thread.sleep(TEMPORARY_SINGLE_NODE_TIMEOUT_MS);
                
                // Re-check if there are other nodes
                allNodes = bootstrapService.getAllKnownNodes();
                if (!allNodes.isEmpty()) {
                    log.info("[DynamicHashAllocator] 🎉 Other nodes detected after waiting! Proceeding with normal allocation");
                    isTemporarySingleNode = false;
                    // Continue with normal hash allocation process
                } else {
                    log.info("[DynamicHashAllocator] ⚠️ Still no other nodes after waiting. Entering temporary single node mode");
                    // Allocate temporary hash but continue monitoring
                    fingerTable.finger.put(proposedHash, CURRENT_NODE_URL);
                    bootstrapService.unregisterBootstrapNode();
                    log.info("[DynamicHashAllocator] 🚀 Temporarily allocated hash: {} to node: {} (monitoring for other nodes)", 
                             proposedHash, CURRENT_NODE_URL);
                    return proposedHash;
                }
            } else {
                // Already in temporary single node mode, return directly
                log.info("[DynamicHashAllocator] 📍 Already in temporary single node mode");
                fingerTable.finger.put(proposedHash, CURRENT_NODE_URL);
                bootstrapService.unregisterBootstrapNode();
                return proposedHash;
            }
        }
        
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
        
        // Use all known nodes for proposal sending
        log.info("[DynamicHashAllocator] Sending proposal to all known nodes: {}", allNodes);
        
        // Send HTTP proposals to all nodes and collect responses immediately
        int acceptedCount = 0;
        
        for (String nodeUrl : allNodes) {
            try {
                log.info("[DynamicHashAllocator] 🌐 Sending HTTP proposal to node: {}", nodeUrl);
                HashAllocationHTTPClient.ProposalResponse response = 
                    httpClient.sendHashProposalWithRetries(nodeUrl, proposalRequest, 3);
                
                if (response.isAccepted()) {
                    acceptedCount++;
                    log.info("[DynamicHashAllocator] ✅ Node {} accepted the proposal", nodeUrl);
                    
                    // Add to confirmation collector for compatibility
                    Set<String> confirmations = proposalAcknowledgments.get(requestId);
                    if (confirmations != null) {
                        confirmations.add(response.getRespondingNode());
                    }
                } else {
                    log.warn("[DynamicHashAllocator] ❌ Node {} rejected the proposal: {}", nodeUrl, response.getReason());
                }
            } catch (Exception e) {
                log.error("[DynamicHashAllocator] Failed to send proposal to {}: {}", nodeUrl, e.getMessage());
            }
        }
        
        log.info("[DynamicHashAllocator] 📊 HTTP Proposal results: {}/{} nodes accepted", acceptedCount, allNodes.size());
        
        // ⏰ Wait for acknowledgment collection period
        log.info("[DynamicHashAllocator] Waiting for network node to confirm proposal...");
        log.info("[DynamicHashAllocator] Request ID: {}, waiting for confirmations from {} nodes", requestId, allNodes.size());
        
        // Reduce wait time to 6 seconds and add periodic checks
        int totalWaitTimeMs = 6000;
        int checkIntervalMs = 1000; // Check every 1 second
        int checksPerformed = 0;
        
        for (int elapsed = 0; elapsed < totalWaitTimeMs; elapsed += checkIntervalMs) {
            Thread.sleep(checkIntervalMs);
            checksPerformed++;
            
            Set<String> currentConfirmations = proposalAcknowledgments.get(requestId);
            int currentCount = currentConfirmations != null ? currentConfirmations.size() : 0;
            
            log.info("[DynamicHashAllocator] ⏱️ Check #{}: {} confirmations received after {}ms", 
                    checksPerformed, currentCount, elapsed + checkIntervalMs);
            
            if (currentConfirmations != null) {
                log.info("[DynamicHashAllocator] 📋 Current confirmations: {}", currentConfirmations);
            }
            
            // Early exit if we have enough confirmations for single-node networks
            if (allNodes.size() <= 1 && currentCount >= 0) {
                log.info("[DynamicHashAllocator] 🚀 Early exit for single/small network");
                break;
            }
        }
        
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
        
        // Send HTTP confirmations to all nodes
        log.info("[DynamicHashAllocator] 🌐 Sending HTTP confirmations to all nodes");
        for (String nodeUrl : allNodes) {
            try {
                boolean success = httpClient.sendHashConfirmation(nodeUrl, proposalRequest);
                if (success) {
                    log.info("[DynamicHashAllocator] ✅ Successfully sent confirmation to {}", nodeUrl);
                } else {
                    log.warn("[DynamicHashAllocator] ⚠️ Failed to send confirmation to {}", nodeUrl);
                }
            } catch (Exception e) {
                log.error("[DynamicHashAllocator] Error sending confirmation to {}: {}", nodeUrl, e.getMessage());
            }
        }
        
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
        try {
            log.info("[DynamicHashAllocator] Handling hash allocation gossip: {} from {}", 
                     message.getMsgType(), message.getSenderId());
            log.debug("[DynamicHashAllocator] Message content: {}", message.getMsgContent());
            
            NodeJoinRequest request = deserializeJoinRequest(message.getMsgContent());
            log.info("[DynamicHashAllocator] Deserialized request - Phase: {}, Node: {}, Hash: {}", 
                     request.getPhase(), request.getNodeUrl(), request.getProposedHash());
            
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
        } catch (Exception e) {
            log.error("[DynamicHashAllocator] Failed to handle hash allocation gossip message: {}", 
                     e.getMessage(), e);
            log.error("[DynamicHashAllocator] Problematic message - Type: {}, Content: {}, Sender: {}", 
                     message.getMsgType(), message.getMsgContent(), message.getSenderId());
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
     * Handle hash proposal (UDP version)
     */
    private void handleHashProposal(NodeJoinRequest request) {
        Integer hash = request.getProposedHash();
        String requesterNode = request.getNodeUrl();
        log.info("[DynamicHashAllocator] 📥 Received hash proposal: {} from: {}", hash, requesterNode);
        
        boolean accepted = false;
        String reason = "";
        
        NodeJoinRequest existingRequest = hashReservations.get(hash);
        String previouslyAcceptedNode = acceptedProposals.get(hash);
        
        if (previouslyAcceptedNode != null && !previouslyAcceptedNode.equals(requesterNode)) {
            accepted = false;
            reason = "Hash already accepted by another node: " + previouslyAcceptedNode;
            log.info("[DynamicHashAllocator] 🛡️ Rejecting hash proposal: {} already accepted by {}", hash, previouslyAcceptedNode);
        } else if (existingRequest == null) {
            // ✅ No conflict, accept reservation
            hashReservations.put(hash, request);
            acceptedProposals.put(hash, requesterNode);
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
                hashReservations.put(hash, request);  
                accepted = false; 
                reason = "Higher priority request, but hash already accepted by: " + previouslyAcceptedNode;
                log.info("[DynamicHashAllocator] 🔄 Higher priority request detected, but not changing accepted status");
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
        log.info("[DynamicHashAllocator] 🚀 Preparing to send confirmation reply to {}: {} ({})", 
                originalRequest.getNodeUrl(), accepted ? "Accepted" : "Rejected", reason);
        
        NodeJoinRequest ackRequest = NodeJoinRequest.builder()
                .nodeUrl(originalRequest.getNodeUrl())      // Original request node
                .phase(NodeJoinRequest.Phase.PROPOSAL_ACK)  // Confirmation phase
                .proposedHash(originalRequest.getProposedHash())
                .timestamp(originalRequest.getTimestamp())  // Keep original timestamp
                .accepted(accepted)                         // Whether accepted
                .respondingNode(CURRENT_NODE_URL)           // Reply node
                .conflictResolver(originalRequest.generateConflictResolver()) // Keep original conflict resolver
                .build();
        
        log.info("[DynamicHashAllocator] 🔗 Built ACK request - ConflictResolver: {}, Hash: {}", 
                ackRequest.generateConflictResolver(), ackRequest.getProposedHash());
        
        GossipMsg ackGossip = GossipMsg.builder()
                .msgType(GossipMsg.Type.HASH_PROPOSAL_ACK)
                .msgContent(serializeJoinRequest(ackRequest))
                .senderId(CURRENT_NODE_URL)
                .timestamp(String.valueOf(System.currentTimeMillis()))
                .build();
        
        // Extract IP from URL for direct sending
        String targetNodeUrl = originalRequest.getNodeUrl();
        try {
            String[] urlParts = targetNodeUrl.split(":");
            String targetIp = urlParts[1].replace("//", "");
            int gossipPort = 9000; // Use gossip port instead of application port
            
            log.info("[DynamicHashAllocator] 🌐 Sending ACK to IP: {}, Port: {}", targetIp, gossipPort);
            log.info("[DynamicHashAllocator] 📦 ACK Message content: {}", ackGossip.getMsgContent());
            
            // Send with multiple retries for better reliability - increase retries to 5
            gossipService.gossipSender.sendGossipWithRetries(ackGossip, targetIp, gossipPort, 5);
            
            log.info("[DynamicHashAllocator] 📤 Sent confirmation reply to {}: {} ({}) with 5 retries", 
                    originalRequest.getNodeUrl(), accepted ? "Accepted" : "Rejected", reason);
        } catch (Exception e) {
            log.error("[DynamicHashAllocator] Failed to send confirmation reply to {}: {}", 
                     originalRequest.getNodeUrl(), e.getMessage(), e);
        }
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
        log.info("[DynamicHashAllocator] 🔍 Processing confirmation for request ID: {}", requestId);
        log.info("[DynamicHashAllocator] 📋 Available pending requests: {}", proposalAcknowledgments.keySet());
        
        Set<String> confirmations = proposalAcknowledgments.get(requestId);
        
        if (confirmations == null) {
            log.warn("[DynamicHashAllocator] ❌ No pending request found for requestId: {}, available requests: {}", 
                    requestId, proposalAcknowledgments.keySet());
            
            // Try to find similar request IDs in case of slight differences
            log.info("[DynamicHashAllocator] 🔍 Searching for similar request IDs containing node URL...");
            for (String availableId : proposalAcknowledgments.keySet()) {
                if (availableId.contains(requesterNode)) {
                    log.info("[DynamicHashAllocator] 🔍 Found similar request ID: {}", availableId);
                }
            }
            return;
        }
        
        if (accepted != null && accepted) {
            boolean added = confirmations.add(respondingNode);
            if (added) {
                log.info("[DynamicHashAllocator] ✅ Added confirmation from {}, current confirmation count: {}/{}", 
                        respondingNode, confirmations.size(), confirmations);
                log.info("[DynamicHashAllocator] 📊 Current confirmations list: {}", confirmations);
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
        try {
            log.debug("[DynamicHashAllocator] Deserializing join request: {}", content);
            String[] parts = content.split("\\|");
            
            if (parts.length < 5) {
                throw new IllegalArgumentException("Invalid join request format, expected at least 5 parts, got: " + parts.length);
            }
            
            NodeJoinRequest.NodeJoinRequestBuilder builder = NodeJoinRequest.builder()
                    .nodeUrl(parts[0])
                    .phase(NodeJoinRequest.Phase.valueOf(parts[1]))
                    .proposedHash("null".equals(parts[2]) ? null : Integer.parseInt(parts[2]))
                    .timestamp(Long.parseLong(parts[3]))
                    .priority(Integer.parseInt(parts[4]));
                    
            // Handle optional fields with bounds checking
            if (parts.length > 5 && !"null".equals(parts[5])) {
                builder.accepted(Boolean.parseBoolean(parts[5]));
            }
            if (parts.length > 6 && !"null".equals(parts[6])) {
                builder.respondingNode(parts[6]);
            }
            if (parts.length > 7 && !"null".equals(parts[7])) {
                builder.conflictResolver(parts[7]);
            }
            
            NodeJoinRequest request = builder.build();
            
            // If conflictResolver is not provided, generate it
            if (request.getConflictResolver() == null) {
                request.setConflictResolver(request.generateConflictResolver());
                log.debug("[DynamicHashAllocator] Generated conflict resolver: {}", request.getConflictResolver());
            }
            
            log.debug("[DynamicHashAllocator] Successfully deserialized join request: Phase={}, Node={}, Hash={}, ConflictResolver={}", 
                     request.getPhase(), request.getNodeUrl(), request.getProposedHash(), request.getConflictResolver());
            return request;
        } catch (Exception e) {
            log.error("[DynamicHashAllocator] Failed to deserialize join request: {}", content);
            log.error("[DynamicHashAllocator] Deserialization error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to deserialize NodeJoinRequest: " + content, e);
        }
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
    
    /**
     * Process hash proposal via HTTP (more reliable than UDP)
     * @param request Hash proposal request
     * @return Proposal result
     */
    public org.distributed.shardingjh.controller.hashallocation.HashAllocationController.ProposalResult 
           processHashProposalHTTP(NodeJoinRequest request) {
        Integer hash = request.getProposedHash();
        String requesterNode = request.getNodeUrl();
        log.info("[DynamicHashAllocator] 📥 Processing HTTP hash proposal: {} from: {}", hash, requesterNode);
        
        boolean accepted = false;
        String reason = "";
        
        NodeJoinRequest existingRequest = hashReservations.get(hash);
        if (existingRequest == null) {
            // ✅ No conflict, accept reservation
            hashReservations.put(hash, request);
            accepted = true;
            reason = "No conflict, accept proposal";
            log.info("[DynamicHashAllocator] ✅ Accepted HTTP hash proposal: {}", hash);
        } else if (existingRequest.getNodeUrl().equals(requesterNode)) {
            // ✅ Same node request, accept
            accepted = true;
            reason = "Same node request, accept";
            log.info("[DynamicHashAllocator] ✅ Repeat HTTP request, accept hash proposal: {}", hash);
        } else {
            // ⚔️ Conflict, compare priority
            if (shouldYieldToConflictingRequest(request)) {
                hashReservations.put(hash, request);  // Yield to higher priority
                accepted = true;
                reason = "Yield to higher priority request";
                log.info("[DynamicHashAllocator] 🔄 HTTP Conflict resolution: Yield to higher priority request");
            } else {
                accepted = false;
                reason = "Conflict with higher priority request";
                log.info("[DynamicHashAllocator] 🛡️ HTTP Conflict resolution: Reject, keep current reservation");
            }
        }
        
        return new org.distributed.shardingjh.controller.hashallocation.HashAllocationController.ProposalResult(
            accepted, reason, CURRENT_NODE_URL);
    }
    
    /**
     * Process hash confirmation via HTTP
     * @param request Hash confirmation request
     */
    public void processHashConfirmationHTTP(NodeJoinRequest request) {
        Integer hash = request.getProposedHash();
        String nodeUrl = request.getNodeUrl();
        
        log.info("[DynamicHashAllocator] Processing HTTP hash confirmation for hash: {} from node: {}", hash, nodeUrl);
        
        // Check for hash conflicts before adding
        String existingNode = fingerTable.finger.get(hash);
        if (existingNode != null && !existingNode.equals(nodeUrl)) {
            log.error("[DynamicHashAllocator] HASH CONFLICT DETECTED! Hash {} already occupied by {} but requested by {}", 
                     hash, existingNode, nodeUrl);
            throw new RuntimeException("Hash conflict detected: " + hash + " already occupied by " + existingNode);
        }
        
        // Official addition to finger table
        fingerTable.finger.put(hash, nodeUrl);
        hashReservations.remove(hash);
        
        log.info("[DynamicHashAllocator] Added node to finger table via HTTP: {} -> {}", hash, nodeUrl);
    }
    
    /**
     * Start isolation prevention scheduler to check for other nodes periodically
     */
    private void startIsolationPreventionScheduler() {
        log.info("[DynamicHashAllocator] 🔄 Starting isolation prevention scheduler");
        
        isolationPreventionScheduler.scheduleAtFixedRate(() -> {
            try {
                if (isTemporarySingleNode) {
                    log.info("[DynamicHashAllocator] 🔍 Checking for other nodes to prevent isolation...");
                    
                    // Rediscover network state
                    List<String> discoveredNodes = rediscoverNetwork();
                    
                    if (!discoveredNodes.isEmpty()) {
                        log.info("[DynamicHashAllocator] 🎉 Discovered other nodes: {}. Rejoining network!", discoveredNodes);
                        rejoinNetwork();
                    } else {
                        log.debug("[DynamicHashAllocator] 🔍 Still no other nodes found, continuing to monitor...");
                    }
                }
            } catch (Exception e) {
                log.error("[DynamicHashAllocator] Error in isolation prevention: {}", e.getMessage(), e);
            }
        }, ISOLATION_PREVENTION_INTERVAL_MS, ISOLATION_PREVENTION_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Rediscover other nodes in the network using HTTP (more reliable than UDP)
     */
    private List<String> rediscoverNetwork() {
        // Re-register as bootstrap node
        bootstrapService.registerBootstrapNode();
        
        // Send HTTP health check requests to configured nodes
        Set<String> configuredNodes = new HashSet<>(); 
        for (String entry : bootstrapService.getConfiguredEntries()) {
            String[] parts = entry.split("=");
            if (parts.length == 2) {
                String nodeUrl = parts[1];
                if (!nodeUrl.equals(CURRENT_NODE_URL)) {
                    configuredNodes.add(nodeUrl);
                }
            }
        }
        
        log.info("[DynamicHashAllocator] 🌐 Sending HTTP health checks to configured nodes: {}", configuredNodes);
        
        // Send HTTP health checks to configured nodes
        for (String nodeUrl : configuredNodes) {
            try {
                log.info("[DynamicHashAllocator] 📡 Checking node availability via HTTP: {}", nodeUrl);
                
                // Try simple health check first
                boolean isHealthy = performSimpleHealthCheck(nodeUrl);
                
                if (isHealthy) {
                    log.info("[DynamicHashAllocator] ✅ Node {} is responsive via HTTP health check", nodeUrl);
                    // Add to bootstrap service as discovered node
                    bootstrapService.handleBootstrapAnnouncement(nodeUrl);
                } else {
                    log.debug("[DynamicHashAllocator] ❌ Node {} not responsive to health check", nodeUrl);
                }
                
            } catch (Exception e) {
                log.debug("[DynamicHashAllocator] Node {} not responsive: {}", nodeUrl, e.getMessage());
            }
        }
        
        // Check if other nodes were discovered
        List<String> discoveredNodes = bootstrapService.getAllKnownNodes();
        log.info("[DynamicHashAllocator] 📊 Discovery results: {} nodes found", discoveredNodes.size());
        return discoveredNodes;
    }
    
    /**
     * Alternative health check using basic HTTP request
     */
    private boolean checkNodeHealthAlternative(String nodeUrl) {
        try {
            // Try heartbeat health endpoint
            log.debug("[DynamicHashAllocator] Trying heartbeat health check for: {}", nodeUrl);
            return performSimpleHealthCheck(nodeUrl);
            
        } catch (Exception e) {
            log.debug("[DynamicHashAllocator] Alternative health check failed for {}: {}", nodeUrl, e.getMessage());
            return false;
        }
    }
    
    /**
     * Perform simple HTTP health check
     */
    private boolean performSimpleHealthCheck(String nodeUrl) {
        try {
            String healthUrl = nodeUrl + "/heartbeat/health";
            
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
            
            org.springframework.http.HttpEntity<String> request = new org.springframework.http.HttpEntity<>(headers);
            
            // Create a simple RestTemplate for this check
            org.springframework.web.client.RestTemplate restTemplate = new org.springframework.web.client.RestTemplate();
            org.springframework.http.client.SimpleClientHttpRequestFactory factory = 
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
            factory.setConnectTimeout(2000); // 2 seconds
            factory.setReadTimeout(3000);    // 3 seconds
            restTemplate.setRequestFactory(factory);
            
            org.springframework.http.ResponseEntity<java.util.Map> response = restTemplate.exchange(
                healthUrl,
                org.springframework.http.HttpMethod.GET,
                request,
                java.util.Map.class
            );
            
            boolean isHealthy = response.getStatusCode() == org.springframework.http.HttpStatus.OK;
            log.debug("[DynamicHashAllocator] Health check for {}: {}", nodeUrl, isHealthy ? "HEALTHY" : "UNHEALTHY");
            return isHealthy;
            
        } catch (Exception e) {
            log.debug("[DynamicHashAllocator] Health check failed for {}: {}", nodeUrl, e.getMessage());
            return false;
        }
    }
    
    /**
     * Rejoin the network and reallocate hash
     */
    private void rejoinNetwork() {
        try {
            log.info("[DynamicHashAllocator] 🔄 Rejoining network and reallocating hash...");
            
            // Stop isolation prevention
            isTemporarySingleNode = false;
            isolationPreventionScheduler.shutdown();
            
            // Clear current hash allocation
            Integer currentHash = null;
            for (Map.Entry<Integer, String> entry : fingerTable.finger.entrySet()) {
                if (entry.getValue().equals(CURRENT_NODE_URL)) {
                    currentHash = entry.getKey();
                    break;
                }
            }
            
            if (currentHash != null) {
                fingerTable.finger.remove(currentHash);
                log.info("[DynamicHashAllocator] 🗑️ Removed temporary hash allocation: {}", currentHash);
            }
            
            // Restart hash allocation
            log.info("[DynamicHashAllocator] 🔄 Starting fresh hash allocation...");
            allocateHashForCurrentNode();
            
        } catch (Exception e) {
            log.error("[DynamicHashAllocator] Failed to rejoin network: {}", e.getMessage(), e);
            // If rejoin fails, continue monitoring
            isTemporarySingleNode = true;
            startIsolationPreventionScheduler();
        }
    }
    
    /**
     * Check if the current node is in temporary single node mode
     */
    public boolean isInTemporarySingleNodeMode() {
        return isTemporarySingleNode;
    }
    
    /**
     * Notify of new node discovery (called by heartbeat or other services)
     */
    public void notifyNodeDiscovered(String nodeUrl) {
        if (isTemporarySingleNode) {
            log.info("[DynamicHashAllocator] 🔔 Notified of new node discovery: {}. Triggering rejoin process.", nodeUrl);
            
            // Asynchronously trigger the rejoin process
            scheduler.schedule(() -> {
                try {
                    rejoinNetwork();
                } catch (Exception e) {
                    log.error("[DynamicHashAllocator] Failed to rejoin network after node discovery: {}", e.getMessage(), e);
                }
            }, 1000, TimeUnit.MILLISECONDS); // 1 second delay
        }
    }
    
    /**
     * Find an available hash using linear search (simple fallback method)
     * @param currentTable Current finger table
     * @return Available hash or null if none found
     */
    private Integer findAvailableHashLinear(Map<Integer, String> currentTable) {
        Set<Integer> reservedHashes = getReservedHashesFromConfig();
        Set<Integer> unavailableHashes = new HashSet<>(currentTable.keySet());
        unavailableHashes.addAll(reservedHashes);
        
        // Linear search for available hash
        for (int hash = 32; hash < 256; hash += 32) {
            if (!unavailableHashes.contains(hash)) {
                log.info("[DynamicHashAllocator] Found available hash via linear search: {}", hash);
                return hash;
            }
        }
        
        log.error("[DynamicHashAllocator] No available hash found via linear search");
        return null;
    }
    
    /**
     * Find an available hash using linear search, excluding specified hashes
     * @param currentTable Current finger table
     * @param excludedHashes Hashes to exclude from consideration
     * @return Available hash or null if none found
     */
    private Integer findAvailableHashLinearExcluding(Map<Integer, String> currentTable, Set<Integer> excludedHashes) {
        Set<Integer> reservedHashes = getReservedHashesFromConfig();
        Set<Integer> unavailableHashes = new HashSet<>(currentTable.keySet());
        unavailableHashes.addAll(reservedHashes);
        unavailableHashes.addAll(excludedHashes);
        
        // Linear search for available hash
        for (int hash = 32; hash < 256; hash += 32) {
            if (!unavailableHashes.contains(hash)) {
                log.info("[DynamicHashAllocator] Found available hash via linear search (excluding {}): {}", excludedHashes, hash);
                return hash;
            }
        }
        
        log.error("[DynamicHashAllocator] No available hash found via linear search excluding: {}", excludedHashes);
        return null;
    }
} 