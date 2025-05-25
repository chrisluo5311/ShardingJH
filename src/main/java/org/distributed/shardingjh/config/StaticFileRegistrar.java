package org.distributed.shardingjh.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.model.LocalFileStore;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.http.HttpHeaders;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Component
public class StaticFileRegistrar {

    @Resource
    private LocalFileStore  localFileStore;

    @Value("${static.path}")
    private String staticFilePath;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    @Resource
    private FingerTable fingerTable;

    private final RestTemplate restTemplate = new RestTemplate();

    @PostConstruct
    public void loadStaticFiles() throws IOException {
        Path staticFolder = Path.of(staticFilePath);

        if (!Files.exists(staticFolder)) {
            System.out.println("📂 Static folder not found: " + staticFolder);
            return;
        }

        Files.list(staticFolder)
                .filter(Files::isRegularFile)
                .map(path -> path.getFileName().toString())
                .forEach(localFileStore::register);

        System.out.println("✅ Registered static files in: " + staticFolder);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void replicateToNextNode() throws IOException {
        if (fingerTable.finger.isEmpty()) {
            throw new IllegalStateException("Finger table not initialized yet.");
        }
        String nextNode = fingerTable.getNextNodeAfter(CURRENT_NODE_URL);

        Path folder = Paths.get(staticFilePath);
        if (!Files.exists(folder)) return;

        Files.list(folder)
                .filter(Files::isRegularFile)
                .filter(path -> !path.getFileName().toString().startsWith("."))
                .forEach(path -> {
            String fileName = path.getFileName().toString();
            try {
                byte[] bytes = Files.readAllBytes(path);
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.MULTIPART_FORM_DATA);

                MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
                body.add("file", new ByteArrayResource(bytes) {
                    @Override public String getFilename() { return fileName; }
                });

                HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(body, headers);
                restTemplate.postForEntity(nextNode + "/static/upload", request, String.class);
                log.info("✅ Replicated file {} to {}", fileName, nextNode);

            } catch (Exception e) {
                log.warn("⚠️ Failed to replicate {} to {}: {}", fileName, nextNode, e.getMessage());
            }
        });
    }
}
