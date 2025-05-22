package org.distributed.shardingjh.model;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class LocalFileStore {

    private final Map<Integer, String> fileHashMap = new HashMap<>();

    public void register(String fileName) {
        int hash = hash(fileName);
        fileHashMap.put(hash, fileName); // filename → actual file path if needed
    }

    public boolean contains(String fileName) {
        return fileHashMap.containsKey(hash(fileName));
    }

    public String get(String fileName) {
        return fileHashMap.get(hash(fileName));
    }

    private int hash(String value) {
        return Math.abs(value.hashCode()) % 256; // Chord-style 8-bit ring
    }
}
