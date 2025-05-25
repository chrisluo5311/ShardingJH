package org.distributed.shardingjh.controller.filecontroller;

import lombok.extern.slf4j.Slf4j;
import jakarta.annotation.Resource;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.p2p.FingerTable;
import org.distributed.shardingjh.model.LocalFileStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@RestController
public class StaticFileController {

    @Resource
    private LocalFileStore fileStore;

    @Resource
    private FingerTable fingerTable;

    @Value("${static.path}")
    private String staticFilePath;

    @Value("${router.server-url}")
    private String CURRENT_NODE_URL;

    private final RestTemplate restTemplate = new RestTemplate();

    // Note: This method is not used in the current implementation
    @RequestMapping(value = "/static/lookup-meta", method = RequestMethod.GET)
    public MgrResponseDto<String> findFile(@RequestParam String fileName) {
        if (fileStore.contains(fileName)) {
            return MgrResponseDto.success(fileName);
        }

        String nextNode = fingerTable.findNextNode(fileName,CURRENT_NODE_URL);
        String url = nextNode + "/static/lookup?fileName=" + URLEncoder.encode(fileName, StandardCharsets.UTF_8);
        log.info("🔁 Forwarding request for {} to {}", fileName, nextNode);
        return restTemplate.getForObject(url, MgrResponseDto.class);
    }

    @RequestMapping(value = "/static/lookup", method = RequestMethod.GET)
    public ResponseEntity<byte[]> serveFile(@RequestParam String fileName) throws IOException {
        log.info("Looking up static file: {}", fileName);
        if (fileStore.contains(fileName)) {
            Path filePath = Path.of(staticFilePath, fileName);
            byte[] fileBytes = Files.readAllBytes(filePath);
            String contentType = Files.probeContentType(filePath);
            log.info("Serving file: {} with content type: {}", fileName, contentType);
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=" + fileName)
                    .contentType(MediaType.parseMediaType(contentType != null ? contentType : "application/octet-stream"))
                    .body(fileBytes);
        }

        // Forward to next node if not found
        String nextNode = fingerTable.findNextNode(fileName, CURRENT_NODE_URL);
        String url = nextNode + "/static/lookup?fileName=" + URLEncoder.encode(fileName, StandardCharsets.UTF_8);
        log.info("🔁 Forwarding request for {} to {}", fileName, url);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                byte[].class
        );
        return ResponseEntity
                .status(response.getStatusCode())
                .headers(response.getHeaders())
                .body(response.getBody());
    }


    @PostMapping("/static/upload")
    public ResponseEntity<String> uploadFile(
            @RequestParam("file") MultipartFile file,
            @RequestHeader(value = "X-Replicated-From", required = false) String replicatedFrom) throws IOException {

        String fileName = file.getOriginalFilename();
        Path dest = Paths.get(staticFilePath, fileName);

        // ✅ If file was received from another node, skip triggering further replication
        if (replicatedFrom != null && !replicatedFrom.isEmpty()) {
            log.info("📥 Received replicated file {} from {}", fileName, replicatedFrom);
            Files.write(dest, file.getBytes());
            fileStore.register(fileName);
            return ResponseEntity.ok("Replicated without re-forwarding");
        }

        // If uploaded locally (not replicated), still accept
        log.info("📥 Received locally uploaded file: {}", fileName);
        Files.write(dest, file.getBytes());
        fileStore.register(file.getOriginalFilename());
        replicateToNextNode(fileName);

        return ResponseEntity.ok("Uploaded");
    }

    private void replicateToNextNode(String fileName) {
        try {
            String nextNode = fingerTable.getNextNodeAfter(CURRENT_NODE_URL);
            Path filePath = Paths.get(staticFilePath, fileName);
            byte[] bytes = Files.readAllBytes(filePath);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);
            headers.set("X-Replicated-From", CURRENT_NODE_URL);

            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            body.add("file", new ByteArrayResource(bytes) {
                @Override public String getFilename() {
                    return fileName;
                }
            });

            HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(body, headers);
            restTemplate.postForEntity(nextNode + "/static/upload", request, String.class);
            log.info("📤 Replicated {} to {}", fileName, nextNode);
        } catch (Exception e) {
            log.warn("⚠️ Replication failed for {}: {}", fileName, e.getMessage());
        }
    }

}