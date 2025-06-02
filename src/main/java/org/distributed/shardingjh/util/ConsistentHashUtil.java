package org.distributed.shardingjh.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import org.distributed.shardingjh.common.constant.ShardConst;

import lombok.extern.slf4j.Slf4j;

/**
 * 一致性哈希工具类，提供更好的hash分布和冲突避免
 */
@Slf4j
public class ConsistentHashUtil {
    
    /**
     * 使用SHA-256生成更均匀的hash值，减少冲突概率
     * @param input 输入字符串
     * @return hash值
     */
    public static int generateSHA256Hash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            
            // 取前4个字节转换为int
            int result = 0;
            for (int i = 0; i < 4; i++) {
                result = (result << 8) + (hash[i] & 0xff);
            }
            
            return Math.abs(result) % ShardConst.FINGER_MAX_RANGE;
        } catch (NoSuchAlgorithmException e) {
            log.error("SHA-256 algorithm not available, falling back to hashCode", e);
            return Math.abs(input.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        }
    }
    
    /**
     * 生成虚拟节点hash值，通过添加后缀来创建多个虚拟节点
     * @param nodeUrl 节点URL
     * @param virtualNodeIndex 虚拟节点索引
     * @return hash值
     */
    public static int generateVirtualNodeHash(String nodeUrl, int virtualNodeIndex) {
        String virtualNode = nodeUrl + "#VN" + virtualNodeIndex;
        return generateSHA256Hash(virtualNode);
    }
    
    /**
     * 为节点生成唯一hash值，包含冲突检测和重试机制
     * @param nodeUrl 节点URL
     * @param existingHashes 已存在的hash值集合
     * @param maxAttempts 最大尝试次数
     * @return 唯一的hash值
     */
    public static int generateUniqueHash(String nodeUrl, Set<Integer> existingHashes, int maxAttempts) {
        // 策略1: 尝试基本SHA-256 hash
        int baseHash = generateSHA256Hash(nodeUrl);
        if (!existingHashes.contains(baseHash)) {
            log.info("[ConsistentHashUtil] Using base SHA-256 hash {} for node {}", baseHash, nodeUrl);
            return baseHash;
        }
        
        // 策略2: 尝试添加时间戳
        long timestamp = System.currentTimeMillis();
        String timestampedUrl = nodeUrl + "@" + timestamp;
        int timestampHash = generateSHA256Hash(timestampedUrl);
        if (!existingHashes.contains(timestampHash)) {
            log.info("[ConsistentHashUtil] Using timestamp-based hash {} for node {}", timestampHash, nodeUrl);
            return timestampHash;
        }
        
        // 策略3: 使用虚拟节点方式重试
        for (int i = 1; i <= maxAttempts; i++) {
            int virtualHash = generateVirtualNodeHash(nodeUrl, i);
            if (!existingHashes.contains(virtualHash)) {
                log.info("[ConsistentHashUtil] Using virtual node hash {} (attempt {}) for node {}", virtualHash, i, nodeUrl);
                return virtualHash;
            }
        }
        
        // 策略4: 最后的线性探测（递增）
        int candidate = (baseHash + 1) % ShardConst.FINGER_MAX_RANGE;
        int attempts = 0;
        while (attempts < ShardConst.FINGER_MAX_RANGE && existingHashes.contains(candidate)) {
            candidate = (candidate + 1) % ShardConst.FINGER_MAX_RANGE;
            attempts++;
        }
        
        if (attempts < ShardConst.FINGER_MAX_RANGE) {
            log.info("[ConsistentHashUtil] Using linear probe hash {} after {} attempts for node {}", candidate, attempts, nodeUrl);
            return candidate;
        }
        
        throw new RuntimeException("Unable to generate unique hash for node " + nodeUrl + " after exhaustive search");
    }
    
    /**
     * 验证hash值分布的均匀性
     * @param hashes hash值集合
     * @return 分布统计信息
     */
    public static String analyzeHashDistribution(Set<Integer> hashes) {
        if (hashes.isEmpty()) {
            return "No hashes to analyze";
        }
        
        int bucketSize = ShardConst.FINGER_MAX_RANGE / 8; // 8个bucket
        int[] buckets = new int[8];
        
        for (Integer hash : hashes) {
            int bucketIndex = Math.min(hash / bucketSize, 7);
            buckets[bucketIndex]++;
        }
        
        StringBuilder sb = new StringBuilder("Hash distribution across 8 buckets: ");
        for (int i = 0; i < 8; i++) {
            sb.append(String.format("B%d:%d ", i, buckets[i]));
        }
        
        return sb.toString();
    }
} 