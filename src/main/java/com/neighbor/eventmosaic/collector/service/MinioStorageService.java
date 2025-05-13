package com.neighbor.eventmosaic.collector.service;

import java.nio.file.Path;

/**
 * Определяет контракт для сервиса взаимодействия с S3-совместимым хранилищем MinIO.
 * Предоставляет методы для загрузки, получения URL и удаления объектов.
 */
public interface MinioStorageService {

    /**
     * Загружает локальный файл в хранилище MinIO.
     * Автоматически определяет размер и Content-Type файла.
     */
    String uploadFile(String objectName, Path filePath);

    /**
     * Получает постоянный URL для доступа к объекту в MinIO.
     * Метод формирует стандартный URL вида: {@code <endpoint>/<bucket>/<objectName>}.
     * Фактическая доступность URL зависит от политики доступа, настроенной для бакета в MinIO.
     */
    String getObjectUrl(String objectName);

    /**
     * Удаляет объект из хранилища MinIO.
     */
    void deleteObject(String objectName);
}