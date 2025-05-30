package org.distributed.shardingjh.p2p;

import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.ShardConst;
import org.springframework.stereotype.Component;

import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class FingerTable {
    public final TreeMap<Integer, String> finger = new TreeMap<>();

    public void addEntry(int hash, String address) {
        finger.put(hash, address);
    }

    /**
     * Find the next node in the finger table for a given file
     * Skip the current node if it is in the finger table
     * Wrap around to the head of the table if no larger node is found
     * @param fileName the name of the file
     * @param currentNodeUrl the URL of the current node
     * */
    public String findNextNode(String fileName, String currentNodeUrl) {
        int target = Math.abs(fileName.hashCode()) % ShardConst.FINGER_MAX_RANGE;
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

    public String getNextNodeAfter(String currentNodeUrl) {
        if (finger.isEmpty()) throw new NoSuchElementException("Finger table is empty");
        boolean found = false;
        for (String node : finger.values()) {
            if (found) return node;
            if (node.equals(currentNodeUrl)) found = true;
        }
        // Wrap around
        return finger.values().iterator().next();
    }

    @Override
    public String toString() {
        return finger.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(","));
    }
}
