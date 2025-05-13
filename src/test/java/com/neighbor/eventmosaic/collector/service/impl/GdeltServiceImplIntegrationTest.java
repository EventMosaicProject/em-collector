package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.client.GdeltClient;
import com.neighbor.eventmosaic.collector.config.TestFileSystemConfig;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveProcessResult;
import com.neighbor.eventmosaic.collector.scheduler.GdeltScheduler;
import com.neighbor.eventmosaic.collector.service.ArchiveService;
import com.neighbor.eventmosaic.collector.service.FileSystemService;
import com.neighbor.eventmosaic.collector.service.HashStoreService;
import com.neighbor.eventmosaic.collector.service.MinioStorageService;
import com.neighbor.eventmosaic.collector.testcontainer.RedisTestContainerInitializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Интеграционные тесты для {@link GdeltServiceImpl}.
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
@Import(TestFileSystemConfig.class)
@DisplayName("Интеграционные тесты для GdeltServiceImpl")
class GdeltServiceImplIntegrationTest implements RedisTestContainerInitializer {

    private static final String TEST_ARCHIVE_FILENAME_1 = "20250323151500.translation.export.CSV.zip";
    private static final String TEST_ARCHIVE_FILENAME_2 = "20250323151500.translation.mentions.CSV.zip";
    private static final String TEST_HASH_1 = "111";
    private static final String TEST_HASH_2 = "222";
    public static final String GDELT_URL = "http://data.gdeltproject.org/gdeltv2/";
    public static final int SIZE_1 = 47284;
    public static final int SIZE_2 = 80433;

    @Autowired
    private GdeltServiceImpl gdeltService;

    @MockitoBean
    private GdeltClient gdeltClient;

    @MockitoBean
    private GdeltScheduler gdeltScheduler;

    @MockitoSpyBean
    private ArchiveService archiveService;

    @MockitoSpyBean
    private HashStoreService hashStoreService;

    @MockitoBean
    private MinioStorageService mockMinioStorageService;

    @Autowired
    private FileSystemService fileSystemService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @TempDir
    Path tempDownloadDir;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(archiveService, "downloadDir", tempDownloadDir.toString());

        // Очищаем Redis перед каждым тестом
        Objects.requireNonNull(redisTemplate.getConnectionFactory())
                .getConnection()
                .serverCommands()
                .flushDb();
    }

    @Test
    @DisplayName("Не должен обрабатывать архивы, если они уже существуют в Redis с тем же хешем")
    void processLatestArchives_noNewArchives_shouldDoNothing() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange
        String gdeltResponse = getGdeltResponse();

        doReturn(gdeltResponse).when(gdeltClient).getLatestArchivesList();

        hashStoreService.storeHash(TEST_ARCHIVE_FILENAME_1, TEST_HASH_1);
        hashStoreService.storeHash(TEST_ARCHIVE_FILENAME_2, TEST_HASH_2);

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get(10, TimeUnit.SECONDS); // Ожидаем завершения асинхронной операции

        // Assert
        verify(gdeltClient, times(1)).getLatestArchivesList();

        // Проверяем, что проверка на изменение хеша была выполнена для обоих архивов
        verify(hashStoreService, times(1)).isNewOrChanged(TEST_ARCHIVE_FILENAME_1, TEST_HASH_1);
        verify(hashStoreService, times(1)).isNewOrChanged(TEST_ARCHIVE_FILENAME_2, TEST_HASH_2);

        // Главная проверка: Убеждаемся, что метод обработки архивов НЕ вызывался ни разу,
        verify(archiveService, never()).processArchiveAsync(any(GdeltArchiveInfo.class));

        // Убеждаемся, что никакие новые хеши не записывались в Redis (только начальные)
        verify(hashStoreService, times(2)).storeHash(anyString(), anyString());

        assertEquals(TEST_HASH_1, hashStoreService.getStoredHash(TEST_ARCHIVE_FILENAME_1), "Хеш первого архива в Redis не должен измениться");
        assertEquals(TEST_HASH_2, hashStoreService.getStoredHash(TEST_ARCHIVE_FILENAME_2), "Хеш второго архива в Redis не должен измениться");
    }

    @Test
    @DisplayName("Должен корректно обрабатывать ошибку при скачивании архива")
    void processLatestArchives_downloadError_shouldHandleGracefully() throws Exception {
        // Arrange
        String gdeltResponse = getGdeltResponse();

        doReturn(gdeltResponse).when(gdeltClient).getLatestArchivesList();
        doReturn(true).when(hashStoreService).isNewOrChanged(anyString(), anyString());

        // Для первого архива эмулируем ошибку скачивания
        GdeltArchiveInfo firstArchiveInfo = buildArchiveInfo(
                TEST_ARCHIVE_FILENAME_1,
                TEST_HASH_1,
                SIZE_1
        );
        CompletableFuture<GdeltArchiveProcessResult> failedFuture = CompletableFuture.completedFuture(
                GdeltArchiveProcessResult.failure(firstArchiveInfo, "Ошибка при скачивании файла")
        );

        // Для второго архива эмулируем успешную обработку
        GdeltArchiveInfo secondArchiveInfo = buildArchiveInfo(
                TEST_ARCHIVE_FILENAME_2,
                TEST_HASH_2,
                SIZE_2
        );
        CompletableFuture<GdeltArchiveProcessResult> successFuture = CompletableFuture.completedFuture(
                GdeltArchiveProcessResult.success(secondArchiveInfo, List.of("some/path"))
        );

        doReturn(failedFuture).when(archiveService).processArchiveAsync(argThat(info ->
                info.fileName().equals(TEST_ARCHIVE_FILENAME_1)));

        doReturn(successFuture).when(archiveService).processArchiveAsync(argThat(info ->
                info.fileName().equals(TEST_ARCHIVE_FILENAME_2)));

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get(10, TimeUnit.SECONDS);

        // Assert
        verify(archiveService, times(1)).processArchiveAsync(argThat(info ->
                info.fileName().equals(TEST_ARCHIVE_FILENAME_1)));

        verify(archiveService, times(1)).processArchiveAsync(argThat(info ->
                info.fileName().equals(TEST_ARCHIVE_FILENAME_2)));

        // Хеш первого архива не был сохранен в Redis (из-за ошибки)
        assertNull(hashStoreService.getStoredHash(TEST_ARCHIVE_FILENAME_1));

        // Второй архив должен быть успешно обработан
        // Главное, что процесс не упал при ошибке с первым архивом и смог обработать второй
        verify(hashStoreService, times(2)).isNewOrChanged(anyString(), anyString());
    }

    @Test
    @DisplayName("Должен корректно обрабатывать ошибку при неверном хеше архива")
    void processLatestArchives_invalidHash_shouldNotProcessArchive() throws Exception {
        // Arrange
        String gdeltResponse = getGdeltResponse();

        doReturn(gdeltResponse).when(gdeltClient).getLatestArchivesList();
        doReturn(true).when(hashStoreService).isNewOrChanged(anyString(), anyString());

        // Для первого архива эмулируем ошибку валидации хеша
        GdeltArchiveInfo firstArchiveInfo = buildArchiveInfo(
                TEST_ARCHIVE_FILENAME_1,
                TEST_HASH_1,
                SIZE_1
        );
        CompletableFuture<GdeltArchiveProcessResult> failedFuture = CompletableFuture.completedFuture(
                GdeltArchiveProcessResult.failure(firstArchiveInfo, "Хеш загруженного файла не совпадает: скачанный_хеш != " + TEST_HASH_1)
        );

        // Для второго архива эмулируем успешную обработку
        GdeltArchiveInfo secondArchiveInfo = buildArchiveInfo(
                TEST_ARCHIVE_FILENAME_2,
                TEST_HASH_2,
                SIZE_2
        );
        CompletableFuture<GdeltArchiveProcessResult> successFuture = CompletableFuture.completedFuture(
                GdeltArchiveProcessResult.success(secondArchiveInfo, List.of("some/path"))
        );

        doReturn(failedFuture).when(archiveService).processArchiveAsync(argThat(info ->
                info.fileName().equals(TEST_ARCHIVE_FILENAME_1)));

        doReturn(successFuture).when(archiveService).processArchiveAsync(argThat(info ->
                info.fileName().equals(TEST_ARCHIVE_FILENAME_2)));

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get(10, TimeUnit.SECONDS); // Ждем завершения с таймаутом

        // Assert
        verify(archiveService, times(1)).processArchiveAsync(argThat(info ->
                info.fileName().equals(TEST_ARCHIVE_FILENAME_1)));

        verify(archiveService, times(1)).processArchiveAsync(argThat(info ->
                info.fileName().equals(TEST_ARCHIVE_FILENAME_2)));

        verify(hashStoreService, times(1)).isNewOrChanged(TEST_ARCHIVE_FILENAME_1, TEST_HASH_1);
        verify(hashStoreService, times(1)).isNewOrChanged(TEST_ARCHIVE_FILENAME_2, TEST_HASH_2);

        // Хеш первого архива не был сохранен в Redis (из-за ошибки хеша)
        assertNull(hashStoreService.getStoredHash(TEST_ARCHIVE_FILENAME_1));

        // Операции должны выполняться в следующем порядке:
        // - isNewOrChanged (проверка нужно ли обрабатывать)
        // - processArchiveAsync (обработка)
        // Так как processArchiveAsync для первого архива вернул ошибку,
        // то финальная запись хеша не должна произойти
        verify(hashStoreService, never()).storeHash(eq(TEST_ARCHIVE_FILENAME_1), anyString());
    }


    private static @NotNull String getGdeltResponse() {
        return """
                %s %s %s%s
                %s %s %s%s
                """.formatted(
                SIZE_1,
                TEST_HASH_1,
                GDELT_URL,
                TEST_ARCHIVE_FILENAME_1,
                SIZE_2,
                TEST_HASH_2,
                GDELT_URL,
                TEST_ARCHIVE_FILENAME_2);
    }

    private GdeltArchiveInfo buildArchiveInfo(String fileName,
                                              String hash,
                                              int size) {
        return new GdeltArchiveInfo(
                fileName,
                GDELT_URL + fileName,
                hash,
                size
        );
    }
}