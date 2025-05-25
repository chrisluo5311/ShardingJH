package org.distributed.shardingjh.p2p;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.NavigableMap;
import java.util.TreeMap;

@Slf4j
@Component
public class FingerTable {
    private final TreeMap<Integer, String> finger = new TreeMap<>();

    public void addEntry(int hash, String address) {
        finger.put(hash, address);
    }

    public String findNextNode(String fileName, String currentNodeUrl) {
        int target = Math.abs(fileName.hashCode()) % 256;
        log.info("Finding next node for file: {} (hash: {})", fileName, target);
        log.info("finger table: {}", finger);

        // tailMap will return >= target (because of true)
        NavigableMap<Integer, String> tailMap = finger.tailMap(target, true);
        for (String node : tailMap.values()) {
            // skip the current node
            if (!node.equals(currentNodeUrl)) return node;
        }

        // Wrap-around: search head of the map
        for (String node : finger.values()) {
            if (!node.equals(currentNodeUrl)) return node;
        }

        return currentNodeUrl;
    }
}
