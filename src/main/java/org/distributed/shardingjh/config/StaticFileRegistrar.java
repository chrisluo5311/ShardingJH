package org.distributed.shardingjh.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.distributed.shardingjh.model.LocalFileStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class StaticFileRegistrar {

    @Resource
    private LocalFileStore  localFileStore;

    @Value("${static.path}")
    private String staticFilePath;

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
}
