package com.neighbor.eventmosaic.collector.service;

import com.neighbor.eventmosaic.collector.dto.ExtractedFileInfo;

import java.nio.file.Path;
import java.util.List;

/**
 * Сервис для отслеживания статуса отправки файлов в Kafka.
 */
public interface FileSendStatusService {

    /**
     * Регистрирует извлеченный файл для отслеживания.
     *
     * @param archiveFileName имя архива
     * @param filePath        путь к файлу
     * @return true, если информация успешно сохранена
     */
    boolean registerFile(String archiveFileName, Path filePath);

    /**
     * Отмечает файл как успешно отправленный.
     *
     * @param filePath путь к файлу
     * @return true, если статус успешно обновлен
     */
    boolean markAsSent(Path filePath);

    /**
     * Получает информацию о файле.
     *
     * @param filePath путь к файлу
     * @return информация о файле или null, если файл не найден
     */
    ExtractedFileInfo getFileInfo(Path filePath);

    /**
     * Получает список всех неотправленных файлов.
     *
     * @return список информации о неотправленных файлах
     */
    List<ExtractedFileInfo> getPendingFiles();
}