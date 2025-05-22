package org.distributed.shardingjh.p2p;

import org.springframework.stereotype.Component;

import java.util.TreeMap;

@Component
public class FingerTable {
    private final TreeMap<Integer, String> finger = new TreeMap<>();

    public void addEntry(int hash, String address) {
        finger.put(hash, address);
    }

    public String findNextNode(String fileName) {
        int target = Math.abs(fileName.hashCode()) % 256;
        return finger.ceilingEntry(target) != null
                ? finger.ceilingEntry(target).getValue()
                : finger.firstEntry().getValue();  // wrap-around
    }
}
