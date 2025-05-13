package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.config.TestFileSystemConfig;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveProcessResult;
import com.neighbor.eventmosaic.collector.event.ArchiveExtractedEvent;
import com.neighbor.eventmosaic.collector.scheduler.GdeltScheduler;
import com.neighbor.eventmosaic.collector.service.ArchiveService;
import com.neighbor.eventmosaic.collector.service.FileSystemService;
import com.neighbor.eventmosaic.collector.service.HashStoreService;
import com.neighbor.eventmosaic.collector.service.MinioStorageService;
import com.neighbor.eventmosaic.collector.testcontainer.RedisTestContainerInitializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Интеграционные тесты для {@link ArchiveServiceImpl}.
 * <p>
 * Эти тесты проверяют функциональность сервиса обработки архивов GDELT,
 * включая загрузку, проверку и распаковку архивов.
 * <p>
 * Тесты используют Testcontainers для запуска Redis в контейнере.
 */
@SpringBootTest
@Testcontainers
@DisplayName("Интеграционные тесты для ArchiveServiceImpl")
@Import(TestFileSystemConfig.class)
@ExtendWith(MockitoExtension.class)
@ActiveProfiles("test")
class ArchiveServiceImplIntegrationTest implements RedisTestContainerInitializer {

    private static final String TEST_ARCHIVE_FILENAME = "gdelt-archive.zip";
    private static final String ACTUAL_FILENAME_IN_ARCHIVE = "20250421073000.translation.mentions.CSV";

    @TempDir
    Path tempDownloadDir;

    @Autowired
    private ArchiveService archiveService;

    @Autowired
    private HashStoreService hashStoreService;

    @Autowired
    private FileSystemService fileSystemService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @MockitoBean
    private GdeltScheduler gdeltScheduler;

    @MockitoBean
    private MinioStorageService mockMinioStorageService;

    @Mock
    private ApplicationEventPublisher mockEventPublisher;

    private Path testArchivePath;
    private GdeltArchiveInfo testArchiveInfo;

    @BeforeEach
    void setUp() throws IOException {
        // Подготовка тестовых данных и директорий
        Path sourceArchive = Paths.get("src/test/resources/data/", TEST_ARCHIVE_FILENAME);
        testArchivePath = tempDownloadDir.resolve(TEST_ARCHIVE_FILENAME);

        if (!Files.exists(sourceArchive)) {
            throw new IllegalStateException("Тестовый архив не найден в src/test/resources/data/" + TEST_ARCHIVE_FILENAME);
        }

        Files.createDirectories(tempDownloadDir);

        Files.copy(sourceArchive, testArchivePath, StandardCopyOption.REPLACE_EXISTING);

        // Устанавливаем пути и подменяем публикатор событий в сервисе
        ReflectionTestUtils.setField(archiveService, "downloadDir", tempDownloadDir.toString());
        ReflectionTestUtils.setField(archiveService, "eventPublisher", mockEventPublisher);

        // Очищаем Redis перед каждым тестом
        Objects.requireNonNull(redisTemplate.getConnectionFactory())
                .getConnection()
                .serverCommands()
                .flushDb();

        String fileHash = fileSystemService.calculateMd5(testArchivePath);
        testArchiveInfo = new GdeltArchiveInfo(
                TEST_ARCHIVE_FILENAME,
                "file://" + testArchivePath.toAbsolutePath(), // Для локального скачивания в тесте
                fileHash,
                Files.size(testArchivePath)
        );
    }

    @Test
    @DisplayName("При успешной обработке архива должны извлечься файлы, загрузиться в MinIO и опубликоваться событие с URL")
    void processArchiveAsync_SuccessfulProcessing_ShouldExtractUploadAndPublishEventWithUrls() throws Exception {
        // Arrange
        String expectedFileUrl = "http://minio-test-host/test-bucket/" + ACTUAL_FILENAME_IN_ARCHIVE;

        when(mockMinioStorageService.uploadFile(anyString(), any(Path.class)))
                .thenReturn(expectedFileUrl);

        // Act
        CompletableFuture<GdeltArchiveProcessResult> future = archiveService.processArchiveAsync(testArchiveInfo);
        GdeltArchiveProcessResult result = future.get(10, TimeUnit.SECONDS);

        // Assert
        assertTrue(result.isSuccess(), "Результат обработки должен быть успешным");
        assertThat(result.extractedFiles())
                .hasSize(1)
                .containsExactly(expectedFileUrl);

        verify(mockMinioStorageService, times(1))
                .uploadFile(eq(ACTUAL_FILENAME_IN_ARCHIVE), any(Path.class));

        ArgumentCaptor<ArchiveExtractedEvent> eventCaptor = ArgumentCaptor.forClass(ArchiveExtractedEvent.class);
        verify(mockEventPublisher, times(1)).publishEvent(eventCaptor.capture());

        ArchiveExtractedEvent capturedEvent = eventCaptor.getValue();
        assertEquals(testArchiveInfo, capturedEvent.archiveInfo(), "Информация об архиве в событии должна совпадать");
        assertThat(capturedEvent.extractedFiles())
                .containsExactlyInAnyOrderElementsOf(result.extractedFiles());

        String storedHash = hashStoreService.getStoredHash(testArchiveInfo.fileName());
        assertEquals(testArchiveInfo.hash(), storedHash, "Хеш должен быть сохранен в Redis");

        assertFalse(Files.exists(testArchivePath), "Исходный архив должен быть удален");
    }

    @Test
    @DisplayName("При некорректном хеше должна возвращаться ошибка, файлы не должны загружаться в MinIO и не должно публиковаться событие")
    void processArchiveAsync_InvalidHash_ShouldReturnFailureAndNotInteractWithMinio() throws Exception {
        // Arrange
        GdeltArchiveInfo archiveWithInvalidHash = new GdeltArchiveInfo(
                testArchiveInfo.fileName(),
                testArchiveInfo.url(),
                "invalid_hash_value",
                testArchiveInfo.size()
        );

        // Act
        CompletableFuture<GdeltArchiveProcessResult> future = archiveService.processArchiveAsync(archiveWithInvalidHash);
        GdeltArchiveProcessResult result = future.get(10, TimeUnit.SECONDS);

        // Assert
        assertFalse(result.isSuccess(), "Результат обработки должен быть неуспешным");
        assertTrue(result.errorMessage().contains("не совпадает"),
                "Сообщение об ошибке должно содержать информацию о несовпадении хеша");

        verify(mockMinioStorageService, never()).uploadFile(anyString(), any(Path.class));
        verify(mockMinioStorageService, never()).deleteObject(anyString());

        String storedHash = hashStoreService.getStoredHash(testArchiveInfo.fileName());
        assertNull(storedHash, "Хеш не должен быть сохранен в Redis");

        verify(mockEventPublisher, never()).publishEvent(any());
    }
}