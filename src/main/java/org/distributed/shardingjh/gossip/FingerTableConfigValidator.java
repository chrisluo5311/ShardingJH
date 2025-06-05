package org.distributed.shardingjh.gossip;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class FingerTableConfigValidator {
    
    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;
    
    @Value("${finger.entries}")
    private String entries;
    
    @PostConstruct
    public void validateConfiguration() {
        log.info("[ConfigValidator] Validating finger table configuration for node: {}", CURRENT_NODE_URL);
        log.info("[ConfigValidator] Configuration entries: {}", entries);
        
        Set<Integer> configuredHashes = new HashSet<>();
        Set<String> configuredNodes = new HashSet<>();
        boolean currentNodeInConfig = false;
        
        try {
            String[] configEntries = entries.split(",");
            for (String entry : configEntries) {
                String[] parts = entry.trim().split("=");
                if (parts.length == 2) {
                    Integer hash = Integer.parseInt(parts[0].trim());
                    String nodeUrl = parts[1].trim();
                    
                    // 检查重复hash
                    if (configuredHashes.contains(hash)) {
                        log.error("[ConfigValidator] ❌ DUPLICATE HASH in configuration: {} appears multiple times", hash);
                        throw new RuntimeException("Configuration error: Duplicate hash " + hash + " in finger.entries");
                    }
                    configuredHashes.add(hash);
                    
                    // 检查重复节点
                    if (configuredNodes.contains(nodeUrl)) {
                        log.error("[ConfigValidator] ❌ DUPLICATE NODE in configuration: {} appears multiple times", nodeUrl);
                        throw new RuntimeException("Configuration error: Duplicate node " + nodeUrl + " in finger.entries");
                    }
                    configuredNodes.add(nodeUrl);
                    
                    // 检查当前节点是否在配置中
                    if (nodeUrl.equals(CURRENT_NODE_URL)) {
                        currentNodeInConfig = true;
                        log.info("[ConfigValidator] ✅ Current node found in configuration with hash: {}", hash);
                    }
                    
                    log.debug("[ConfigValidator] Valid configuration entry: {} -> {}", hash, nodeUrl);
                } else {
                    log.error("[ConfigValidator] ❌ INVALID ENTRY format: {}", entry);
                    throw new RuntimeException("Configuration error: Invalid entry format " + entry);
                }
            }
            
            log.info("[ConfigValidator] Configuration validation completed:");
            log.info("[ConfigValidator] - Total configured nodes: {}", configuredNodes.size());
            log.info("[ConfigValidator] - Configured hashes: {}", configuredHashes);
            log.info("[ConfigValidator] - Current node in config: {}", currentNodeInConfig);
            
            if (!currentNodeInConfig) {
                log.warn("[ConfigValidator] ⚠️ WARNING: Current node {} is NOT in finger.entries configuration", CURRENT_NODE_URL);
                log.warn("[ConfigValidator] ⚠️ This node will use dynamic hash allocation, which may cause duplicate assignments");
                log.warn("[ConfigValidator] ⚠️ Consider adding this node to finger.entries configuration for stability");
                log.warn("[ConfigValidator] ⚠️ Suggested addition: <hash>={}", CURRENT_NODE_URL);
            } else {
                log.info("[ConfigValidator] ✅ Configuration validation passed for current node");
            }
            
        } catch (NumberFormatException e) {
            log.error("[ConfigValidator] ❌ Invalid hash value in configuration: {}", e.getMessage());
            throw new RuntimeException("Configuration error: Invalid hash value in finger.entries", e);
        } catch (Exception e) {
            log.error("[ConfigValidator] ❌ Configuration validation failed: {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Check if current node is configured in finger.entries
     * @return true if current node is in configuration
     */
    public boolean isCurrentNodeConfigured() {
        try {
            String[] configEntries = entries.split(",");
            for (String entry : configEntries) {
                String[] parts = entry.trim().split("=");
                if (parts.length == 2 && parts[1].trim().equals(CURRENT_NODE_URL)) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("[ConfigValidator] Error checking node configuration: {}", e.getMessage());
        }
        return false;
    }
    
    /**
     * Get suggested hash value for current node if not configured
     * @return suggested hash value
     */
    public Integer getSuggestedHashForCurrentNode() {
        Set<Integer> usedHashes = new HashSet<>();
        
        try {
            String[] configEntries = entries.split(",");
            for (String entry : configEntries) {
                String[] parts = entry.trim().split("=");
                if (parts.length == 2) {
                    usedHashes.add(Integer.parseInt(parts[0].trim()));
                }
            }
        } catch (Exception e) {
            log.error("[ConfigValidator] Error parsing configuration for suggestion: {}", e.getMessage());
            return 32; // fallback
        }
        
        // 建议一个不冲突的hash值
        for (int hash = 32; hash < 256; hash += 32) {
            if (!usedHashes.contains(hash)) {
                return hash;
            }
        }
        
        // 如果32的倍数都被占用，尝试其他值
        for (int hash = 16; hash < 256; hash += 16) {
            if (!usedHashes.contains(hash)) {
                return hash;
            }
        }
        
        return null; // 无可用hash
    }
} 