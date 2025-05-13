package com.neighbor.eventmosaic.collector.service;

import com.neighbor.eventmosaic.collector.dto.ExtractedFileInfo;

import java.util.List;

/**
 * Сервис для отслеживания статуса отправки файлов в Kafka.
 */
public interface FileSendStatusService {

    /**
     * Регистрирует извлеченный файл для отслеживания.
     *
     * @param archiveFileName имя архива
     * @param fileUrl        URL файла в хранилище
     * @return true, если информация успешно сохранена
     */
    boolean registerFile(String archiveFileName, String fileUrl);

    /**
     * Отмечает файл как успешно отправленный.
     *
     * @param fileUrl URL файла
     * @return true, если статус успешно обновлен
     */
    boolean markAsSent(String fileUrl);

    /**
     * Получает информацию о файле.
     *
     * @param fileUrl URL файла
     * @return информация о файле или null, если файл не найден
     */
    ExtractedFileInfo getFileInfo(String fileUrl);

    /**
     * Получает список всех неотправленных файлов.
     *
     * @return список информации о неотправленных файлах
     */
    List<ExtractedFileInfo> getPendingFiles();
}