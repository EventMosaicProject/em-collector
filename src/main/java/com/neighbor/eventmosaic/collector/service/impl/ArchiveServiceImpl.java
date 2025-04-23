package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveProcessResult;
import com.neighbor.eventmosaic.collector.event.ArchiveExtractedEvent;
import com.neighbor.eventmosaic.collector.exception.GdeltProcessingException;
import com.neighbor.eventmosaic.collector.service.ArchiveService;
import com.neighbor.eventmosaic.collector.service.FileSystemService;
import com.neighbor.eventmosaic.collector.service.HashStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Реализация сервиса обработки архивов GDELT.
 * Обеспечивает асинхронную загрузку, проверку и распаковку архивов.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ArchiveServiceImpl implements ArchiveService, ApplicationEventPublisherAware {

    @Value("${gdelt.storage.download-dir}")
    private String downloadDir;

    @Value("${gdelt.storage.extract-dir}")
    private String extractDir;

    private final FileSystemService fileSystemService;
    private final HashStoreService hashStoreService;

    private ApplicationEventPublisher eventPublisher;

    /**
     * Асинхронно обрабатывает архив GDELT с соблюдением порядка операций.
     * <p>
     * Важно! Операции выполняются в следующем порядке для обеспечения целостности данных:
     * 1. Создание необходимых директорий
     * 2. Загрузка архива
     * 3. Проверка хеша загруженного файла
     * 4. Распаковка архива
     * 5. И только после успешного выполнения всех предыдущих шагов:
     * - Сохранение хеша в Redis (для предотвращения повторной обработки)
     * - Удаление исходного архива
     * <p>
     * Такой порядок гарантирует, что архив не будет помечен как обработанный,
     * если распаковка не завершилась успешно.
     * Также, при успешной распаковки будет опубликовано событие.
     *
     * @param archive Информация об архиве для обработки
     * @return CompletableFuture с результатом обработки архива
     */
    @Override
    public CompletableFuture<GdeltArchiveProcessResult> processArchiveAsync(GdeltArchiveInfo archive) {
        log.info("Начало асинхронной обработки архива: {}", archive.fileName());

        return CompletableFuture.supplyAsync(() -> {
            try {
                createDirs(); // Создание директорий

                Path archivePath = downloadArchive(archive); // Загрузка архива

                verifyFileHash(archivePath, archive); // Проверка хеша загруженного файла

                List<Path> extractedFiles = extractArchive(archivePath, archive); // Распаковка архива

                finalizeProcessing(archivePath, archive); // Сохранение хеша и удаление архива (только после успешной распаковки)

                log.info("Архив успешно обработан: {}", archive.fileName());
                return GdeltArchiveProcessResult.success(archive, extractedFiles);

            } catch (Exception e) {
                log.error("Ошибка при обработке архива {}: {}", archive.fileName(), e.getMessage(), e);
                return GdeltArchiveProcessResult.failure(archive, e.getMessage());
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
     * Создает необходимые директории для скачивания и распаковки файлов.
     *
     * @throws IOException если не удалось создать директории
     */
    private void createDirs() throws IOException {
        fileSystemService.checkAndCreateDirectory(downloadDir);
        fileSystemService.checkAndCreateDirectory(extractDir);
        log.debug("Директории для обработки архивов подготовлены");
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
     * Извлекает файлы из архива.
     * Публикует событие после успешной распаковки.
     *
     * @param archivePath путь к архиву
     * @param archive     информация об архиве
     * @return список путей к извлеченным файлам
     * @throws IOException если возникла ошибка при распаковке
     */
    private List<Path> extractArchive(Path archivePath, GdeltArchiveInfo archive) throws IOException {
        log.debug("Распаковка архива {} в {}", archive.fileName(), extractDir);
        List<Path> pathToExtractFile = fileSystemService.extractZipFile(archivePath, Paths.get(extractDir));

        // Публикация события после успешной распаковки
        eventPublisher.publishEvent(new ArchiveExtractedEvent(archive, pathToExtractFile));
        log.debug("Опубликовано событие о распаковке архива {}", archive.fileName());

        return pathToExtractFile;
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
}