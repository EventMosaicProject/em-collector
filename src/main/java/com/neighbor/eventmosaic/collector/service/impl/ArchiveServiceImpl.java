package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveProcessResult;
import com.neighbor.eventmosaic.collector.event.ArchiveExtractedEvent;
import com.neighbor.eventmosaic.collector.exception.GdeltProcessingException;
import com.neighbor.eventmosaic.collector.service.ArchiveService;
import com.neighbor.eventmosaic.collector.service.FileSystemService;
import com.neighbor.eventmosaic.collector.service.HashStoreService;
import com.neighbor.eventmosaic.collector.service.MinioStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Реализация сервиса обработки архивов GDELT.
 * Обеспечивает асинхронную загрузку, проверку, распаковку,
 * сохранение распакованных файлов в MinIO и публикацию события с URL файлов.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ArchiveServiceImpl implements ArchiveService, ApplicationEventPublisherAware {

    @Value("${gdelt.storage.download-dir}")
    private String downloadDir;

    private final FileSystemService fileSystemService;
    private final HashStoreService hashStoreService;
    private final MinioStorageService minioStorageService;

    private ApplicationEventPublisher eventPublisher;

    /**
     * Асинхронно обрабатывает архив GDELT с соблюдением порядка операций.
     * <p>
     * Важно! Операции выполняются в следующем порядке для обеспечения целостности данных:
     * 1. Создание директории для скачивания.
     * 2. Загрузка архива.
     * 3. Проверка хеша загруженного файла.
     * 4. Распаковка архива во временную директорию.
     * 5. Загрузка каждого распакованного файла в MinIO.
     * 6. Удаление временных локальных файлов.
     * 7. Публикация события с URL файлов из MinIO.
     * 8. Сохранение хеша архива в Redis.
     * 9. Удаление исходного архива.
     * <p>
     * Такой порядок гарантирует, что архив не будет помечен как обработанный,
     * если распаковка не завершилась успешно.
     * Также, при успешной распаковке будет опубликовано событие.
     *
     * @param archive Информация об архиве для обработки
     * @return CompletableFuture с результатом обработки архива
     */
    @Override
    public CompletableFuture<GdeltArchiveProcessResult> processArchiveAsync(GdeltArchiveInfo archive) {
        log.info("Начало асинхронной обработки архива: {}", archive.fileName());

        // Создаем временную директорию для распаковки внутри downloadDir
        Path tempExtractDir = createTempExtractDirectory(archive.fileName());

        return CompletableFuture.supplyAsync(() -> {
            try {
                fileSystemService.checkAndCreateDirectory(downloadDir); // Создание директории для скачивания (директория для распаковки создается динамически)
                log.debug("Директория для скачивания подготовлена: {}", downloadDir);

                Path archivePath = downloadArchive(archive); // Загрузка архива

                verifyFileHash(archivePath, archive); // Проверка хеша загруженного файла

                List<String> extractedFileUrls = extractAndUploadToMinio(archivePath, tempExtractDir, archive); // Распаковка, загрузка в MinIO и удаление локальных файлов

                publishExtractedEvent(archive, extractedFileUrls); // Публикация события с URL

                finalizeProcessing(archivePath, archive); // Сохранение хеша и удаление архива (только после успешной распаковки)

                log.info("Архив успешно обработан: {}", archive.fileName());
                return GdeltArchiveProcessResult.success(archive, extractedFileUrls);

            } catch (Exception e) {
                log.error("Ошибка при обработке архива {}: {}", archive.fileName(), e.getMessage(), e);
                return GdeltArchiveProcessResult.failure(archive, e.getMessage());

            } finally {
                cleanupTempDirectory(tempExtractDir); // Очищаем временную директорию распаковки в любом случае
            }
        }).exceptionally(ex -> {
            log.error("Критическая ошибка при обработке архива {}: {}",
                    archive.fileName(), ex.getMessage(), ex);
            return GdeltArchiveProcessResult.failure(archive, ex.getMessage());
        });
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }


    /**
     * Создает уникальную временную директорию для распаковки архива.
     * Использует имя архива для предотвращения коллизий.
     *
     * @param archiveFileName Имя файла архива.
     * @return Путь к созданной временной директории.
     * @throws GdeltProcessingException если не удалось создать директорию.
     */
    private Path createTempExtractDirectory(String archiveFileName) {
        try {
            // Создаем поддиректорию внутри downloadDir
            Path tempDir = Paths.get(downloadDir, "temp_extract_" + archiveFileName + "_" + System.currentTimeMillis());
            Files.createDirectories(tempDir);
            log.debug("Создана временная директория для распаковки: {}", tempDir);
            return tempDir;

        } catch (IOException e) {
            log.error("Не удалось создать временную директорию для распаковки архива {}: {}", archiveFileName, e.getMessage(), e);
            throw new GdeltProcessingException("Не удалось создать временную директорию для распаковки", e);
        }
    }

    /**
     * Загружает архив по-указанному URL.
     *
     * @param archive информация об архиве
     * @return путь к загруженному файлу
     * @throws IOException если возникла ошибка при загрузке файла
     */
    private Path downloadArchive(GdeltArchiveInfo archive) throws IOException {
        String targetPath = "%s/%s".formatted(downloadDir, archive.fileName());
        log.debug("Загрузка архива {} в {}", archive.url(), targetPath);
        return fileSystemService.downloadFile(archive.url(), targetPath);
    }

    /**
     * Проверяет соответствие хеша загруженного файла ожидаемому хешу.
     *
     * @param archivePath путь к загруженному архиву
     * @param archive     информация об архиве с ожидаемым хешем
     * @throws IOException              если возникла ошибка при расчете хеша
     * @throws GdeltProcessingException если хеш не соответствует ожидаемому
     */
    private void verifyFileHash(Path archivePath, GdeltArchiveInfo archive) throws IOException {
        log.debug("Проверка хеша архива {}", archive.fileName());
        String downloadedFileHash = fileSystemService.calculateMd5(archivePath);
        if (!downloadedFileHash.equals(archive.hash())) {
            throw new GdeltProcessingException("Хеш загруженного файла не совпадает: " + downloadedFileHash + " != " + archive.hash());
        }
        log.debug("Хеш архива {} проверен успешно", archive.fileName());
    }

    /**
     * Извлекает файлы из архива во временную директорию, загружает их в MinIO
     * и удаляет локальные копии.
     *
     * @param archivePath    Путь к архиву.
     * @param tempExtractDir Временная директория для распаковки.
     * @param archive        Информация об архиве (для логирования и формирования имени объекта).
     * @return Список URL загруженных в MinIO файлов.
     */
    private List<String> extractAndUploadToMinio(Path archivePath,
                                                 Path tempExtractDir,
                                                 GdeltArchiveInfo archive) throws IOException {

        log.debug("Распаковка архива {} во временную директорию {}", archive.fileName(), tempExtractDir);
        List<Path> localExtractedFiles = fileSystemService.extractZipFile(archivePath, tempExtractDir);
        log.info("Архив {} распакован, {} файлов извлечено локально.", archive.fileName(), localExtractedFiles.size());

        List<String> uploadedFileUrls = new ArrayList<>();
        try {
            for (Path localFile : localExtractedFiles) {

                String objectName = localFile.getFileName().toString(); // Формируем имя объекта в MinIO (используем имя файла)
                log.debug("Загрузка файла {} как объект '{}' в MinIO...", localFile, objectName);

                String fileUrl = minioStorageService.uploadFile(objectName, localFile); // Загружаем файл в MinIO
                uploadedFileUrls.add(fileUrl);
                log.info("Файл {} успешно загружен в MinIO. URL: {}", localFile, fileUrl);

                deleteLocalFile(localFile);
            }
        } catch (Exception e) {
            log.error("Ошибка при загрузке файла из {} в MinIO. Попытка очистки уже загруженных файлов...", archive.fileName(), e);
            cleanupMinioUploads(uploadedFileUrls);
            throw new GdeltProcessingException("Неожиданная ошибка при загрузке в MinIO", e);
        }

        log.debug("Все файлы из архива {} загружены в MinIO.", archive.fileName());
        return uploadedFileUrls;
    }

    /**
     * Публикует событие об успешной распаковке и загрузке файлов в MinIO.
     *
     * @param archive           Информация об архиве.
     * @param extractedFileUrls Список URL файлов в MinIO.
     */
    private void publishExtractedEvent(GdeltArchiveInfo archive,
                                       List<String> extractedFileUrls) {
        eventPublisher.publishEvent(new ArchiveExtractedEvent(archive, extractedFileUrls));
        log.debug("Опубликовано событие {} с {} URL файлов из MinIO для архива {}",
                ArchiveExtractedEvent.class.getSimpleName(),
                extractedFileUrls.size(),
                archive.fileName());
    }

    /**
     * Завершает обработку архива: сохраняет хеш в Redis и удаляет исходный архив.
     * Этот метод должен вызываться только после успешного извлечения файлов.
     *
     * @param archivePath путь к архиву
     * @param archive     информация об архиве
     * @throws IOException если возникла ошибка при удалении архива
     */
    private void finalizeProcessing(Path archivePath, GdeltArchiveInfo archive) throws IOException {
        // Сохраняем хеш в Redis, чтобы избежать повторной обработки
        hashStoreService.storeHash(archive.fileName(), archive.hash());
        log.debug("Хеш архива {} сохранен в Redis", archive.fileName());

        // Удаляем исходный архив для экономии места
        Files.deleteIfExists(archivePath);
        log.debug("Исходный архив {} удален", archive.fileName());
    }

    /**
     * Безопасно удаляет локальный файл.
     *
     * @param localFile Путь к файлу для удаления.
     */
    private void deleteLocalFile(Path localFile) {
        try {
            Files.deleteIfExists(localFile);
            log.debug("Локальный файл {} удален после загрузки в MinIO.", localFile);
        } catch (IOException e) {
            // Логируем ошибку, но не прерываем процесс,
            // так как основная цель (загрузка в MinIO) достигнута.
            log.warn("Не удалось удалить временный локальный файл {}: {}", localFile, e.getMessage());
        }
    }

    /**
     * Удаляет временную директорию, использовавшуюся для распаковки.
     * Вызывается в блоке finally для гарантии очистки.
     *
     * @param tempDir Путь к временной директории.
     */
    private void cleanupTempDirectory(Path tempDir) {
        if (tempDir == null || !Files.exists(tempDir)) {
            return;
        }
        try {
            FileUtils.deleteDirectory(tempDir.toFile());
            log.debug("Временная директория распаковки {} успешно удалена.", tempDir);
        } catch (IOException e) {
            log.warn("Не удалось полностью очистить временную директорию {}: {}", tempDir, e.getMessage());
        }
    }

    /**
     * Пытается удалить объекты из MinIO, если произошла ошибка во время загрузки группы файлов.
     *
     * @param uploadedUrls Список URL уже загруженных объектов.
     */
    private void cleanupMinioUploads(List<String> uploadedUrls) {
        if (uploadedUrls == null || uploadedUrls.isEmpty()) {
            return;
        }
        log.warn("Начало очистки {} объектов из MinIO из-за ошибки загрузки...", uploadedUrls.size());
        for (String url : uploadedUrls) {
            try {
                // Извлекаем objectName из URL (ожидаемый формат endpoint/bucket/objectName)
                String[] parts = url.split("/");
                if (parts.length >= 3) {
                    String objectName = parts[parts.length - 1]; // Берем последнюю часть
                    minioStorageService.deleteObject(objectName);
                    log.debug("Удален объект '{}' из MinIO при откате.", objectName);
                } else {
                    log.warn("Не удалось извлечь имя объекта из URL '{}' для удаления при откате.", url);
                }
            } catch (Exception e) {
                log.error("Ошибка при попытке удалить объект по URL '{}' из MinIO во время отката: {}", url, e.getMessage(), e);
                // Продолжаем попытки удалить остальные
            }
        }
        log.warn("Очистка объектов MinIO завершена.");
    }
}