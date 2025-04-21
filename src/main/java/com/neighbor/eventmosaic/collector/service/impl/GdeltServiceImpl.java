package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.client.GdeltClient;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveProcessResult;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveType;
import com.neighbor.eventmosaic.collector.service.ArchiveService;
import com.neighbor.eventmosaic.collector.service.GdeltService;
import com.neighbor.eventmosaic.collector.service.HashStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Реализация сервиса для работы с данными GDELT.
 * Обеспечивает получение и обработку архивов GDELT.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GdeltServiceImpl implements GdeltService {

    private final GdeltClient gdeltClient;
    private final HashStoreService hashStoreService;
    private final ArchiveService archiveService;

    /**
     * Запускает процесс обработки последних доступных архивов GDELT.
     * Включает получение списка архивов, выбор тех, которые обрабатываем,
     * и их асинхронную обработку.
     *
     * @return CompletableFuture, который завершается, когда обработка всех архивов завершена
     */
    @Override
    public CompletableFuture<Void> processLatestArchives() {
        log.info("Запуск процесса обработки последних архивов GDELT");

        try {
            // Получение списка архивов
            List<GdeltArchiveInfo> archives = fetchLatestArchives();
            log.info("Получено архивов: {}", archives.size());

            if (archives.isEmpty()) {
                log.info("Список архивов пуст. Нечего обрабатывать.");
                return CompletableFuture.completedFuture(null);
            }

            // Выбор архивов из Redis для обработки
            List<GdeltArchiveInfo> archivesToProcess = selectArchivesToProcess(archives);
            log.info("Архивов для обработки: {}", archivesToProcess.size());

            if (archivesToProcess.isEmpty()) {
                log.info("Нет новых архивов для обработки");
                return CompletableFuture.completedFuture(null);
            }

            // Асинхронная обработка архивов
            return processArchivesBatch(archivesToProcess);

        } catch (Exception e) {
            log.error("Ошибка при обработке архивов GDELT: {}", e.getMessage(), e);
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    /**
     * Запускает асинхронную обработку пакета архивов и анализирует результаты.
     *
     * @param archives список архивов для обработки
     * @return CompletableFuture, который завершается, когда обработка всех архивов завершена
     */
    private CompletableFuture<Void> processArchivesBatch(List<GdeltArchiveInfo> archives) {
        List<CompletableFuture<GdeltArchiveProcessResult>> futures = archives.stream()
                .map(archiveService::processArchiveAsync)
                .toList();

        // Ожидание завершения всех задач
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenAccept(v -> analyzeProcessingResults(futures));
    }

    /**
     * Анализирует результаты обработки архивов и выводит статистику.
     *
     * @param futures список CompletableFuture с результатами обработки
     */
    private void analyzeProcessingResults(List<CompletableFuture<GdeltArchiveProcessResult>> futures) {
        long successCount = futures.stream()
                .map(CompletableFuture::join)
                .filter(GdeltArchiveProcessResult::isSuccess)
                .count();

        log.info("Обработка архивов завершена. Успешно: {}/{}",
                successCount, futures.size());
    }

    /**
     * Получает список последних доступных архивов GDELT.
     *
     * @return Список объектов GdeltArchiveInfo
     */
    private List<GdeltArchiveInfo> fetchLatestArchives() {
        log.debug("Запрос последних архивов GDELT");
        String content = gdeltClient.getLatestArchivesList();
        log.debug("Получен ответ от GDELT API: {}", content);

        return Arrays.stream(content.split("\n"))
                .map(this::parseArchiveInfo)
                .filter(Objects::nonNull)
                .filter(this::isSupportedArchiveType)
                .toList();
    }

    /**
     * Проверяет, является ли архив поддерживаемого типа.
     *
     * @param archive информация об архиве
     * @return true, если архив поддерживаемого типа, иначе false
     */
    private boolean isSupportedArchiveType(GdeltArchiveInfo archive) {
        log.debug("Проверка типа архива: {}", archive.fileName());
        String url = archive.url();

        return Arrays.stream(GdeltArchiveType.values())
                .map(GdeltArchiveType::getPattern)
                .anyMatch(pattern -> pattern.matcher(url).find());
    }

    /**
     * Разбирает строку с информацией об архиве.
     *
     * @param line Строка в формате "размер хеш URL"
     * @return Объект GdeltArchiveInfo или null, если строка некорректна
     */
    private GdeltArchiveInfo parseArchiveInfo(String line) {
        try {
            String[] parts = line.trim().split("\\s+");
            if (parts.length < 3) {
                log.warn("Некорректный формат строки: {}", line);
                return null;
            }

            long size = Long.parseLong(parts[0]);
            String hash = parts[1];
            String url = parts[2];
            String fileName = url.substring(url.lastIndexOf('/') + 1);

            return new GdeltArchiveInfo(fileName, url, hash, size);
        } catch (Exception e) {
            log.error("Ошибка при разборе строки архива: {}", line, e);
            return null;
        }
    }

    /**
     * Выбирает архивы, которые требуют обработки.
     * Выбираются только новые или изменившиеся архивы.
     *
     * @param archives Список всех архивов
     * @return Список архивов, требующих обработки
     */
    private List<GdeltArchiveInfo> selectArchivesToProcess(List<GdeltArchiveInfo> archives) {
        return archives.stream()
                .filter(archive -> hashStoreService.isNewOrChanged(archive.fileName(), archive.hash()))
                .toList();
    }
}