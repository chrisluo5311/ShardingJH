package org.distributed.shardingjh.p2p;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Finger table initializer
 * {
 *  64:  "http://3.147.58.62:8081",
 *  128: "http://3.15.149.110:8082",
 *  192: "http://52.15.151.104:8083"
 *  }
 * */
@Component
public class FingerTableInitializer {

    @Resource
    private FingerTable fingerTable;

    @Value("${finger.entries}")
    private String entries;

    @PostConstruct
    public void init() {
        for (String entry : entries.split(",")) {
            String[] parts = entry.split("=");
            fingerTable.addEntry(Integer.parseInt(parts[0]), parts[1]);
        }
    }
}
