package org.distributed.shardingjh.controller.filecontroller;

import lombok.extern.slf4j.Slf4j;
import jakarta.annotation.Resource;
import org.distributed.shardingjh.common.response.MgrResponseDto;
import org.distributed.shardingjh.p2p.FingerTable;
import org.distributed.shardingjh.model.LocalFileStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) throws IOException {
        Path dest = Paths.get(staticFilePath, file.getOriginalFilename());
        Files.write(dest, file.getBytes());
        fileStore.register(file.getOriginalFilename());
        log.info("📥 Received replica file: {}, saving to {}", file.getOriginalFilename(), dest.toString());
        return ResponseEntity.ok("Uploaded");
    }

}