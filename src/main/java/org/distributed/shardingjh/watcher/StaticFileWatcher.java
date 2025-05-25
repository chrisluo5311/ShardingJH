package org.distributed.shardingjh.watcher;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class StaticFileWatcher implements Runnable {

    @Resource
    private FingerTable fingerTable;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Value("${static.path}")
    private String staticFilePath;

    private final RestTemplate restTemplate = new RestTemplate();

    @PostConstruct
    public void startWatching() throws IOException {
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void run() {
        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            Path path = Paths.get(staticFilePath);
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            log.info("📡 Watching directory for new static files: {}", path);

            while (true) {
                WatchKey key = watchService.take(); // blocks until event occurs
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                        String fileName = event.context().toString();
                        log.info("🆕 Detected new file: {}", fileName);
                        replicateFileToNextNode(fileName);
                    }
                }
                key.reset();
            }
        } catch (Exception e) {
            log.error("❌ File watcher error: {}", e.getMessage());
        }
    }

    private void replicateFileToNextNode(String fileName) {
        try {
            String nextNode = fingerTable.getNextNodeAfter(CURRENT_NODE_URL);
            Path filePath = Paths.get(staticFilePath, fileName);
            byte[] bytes = Files.readAllBytes(filePath);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);

            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            body.add("file", new ByteArrayResource(bytes) {
                @Override public String getFilename() { return fileName; }
            });

            HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(body, headers);
            restTemplate.postForEntity(nextNode + "/static/upload", request, String.class);
            log.info("📤 Auto-replicated {} to {}", fileName, nextNode);
        } catch (Exception e) {
            log.warn("⚠️ Auto-replication failed for {}: {}", fileName, e.getMessage());
        }
    }
}
