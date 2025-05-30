package org.distributed.shardingjh;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetIPTest {

    private static final String CURRENT_NODE_URL = "http://3.15.149.110:8082";

    public void getCurrentIp() {
        String[] parts = CURRENT_NODE_URL.split(":");
        String ip = parts[1].replace("//", "");
        log.info("Current IP: {}", ip);

    }

    public void getCurrentPort() {
        String[] parts = CURRENT_NODE_URL.split(":");
        Integer port = Integer.parseInt(parts[2]);
        log.info("Current Port: {}", port);

    }

    public static void main(String[] args) {
        GetIPTest test = new GetIPTest();
        test.getCurrentIp();
        test.getCurrentPort();
    }
}
