package com.neighbor.eventmosaic.collector.config;

import com.neighbor.eventmosaic.collector.service.FileSystemService;
import com.neighbor.eventmosaic.collector.service.impl.FileSystemServiceImpl;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Конфиг класс для сервиса FileSystemService
 */
@TestConfiguration
public class TestFileSystemConfig {

    /**
     * Подменяет FileSystemServiceImpl на тестовый.
     * Разница в том, что тестовый сервис не скачивает файлы по url,
     * а просто создает их в нужном месте.
     *
     * @return тестовый сервис FileSystemServiceImpl
     */
    @Bean
    @Primary
    public FileSystemService testFileSystemService() {
        return new FileSystemServiceImpl() {
            @Override
            public Path downloadFile(String url, String targetPath) throws IOException {
                if (url.startsWith("file://")) {
                    Path source = Paths.get(url.substring(7));
                    Path target = Paths.get(targetPath);
                    Files.createDirectories(target.getParent());
                    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                    return target;
                }
                return super.downloadFile(url, targetPath);
            }
        };
    }
}