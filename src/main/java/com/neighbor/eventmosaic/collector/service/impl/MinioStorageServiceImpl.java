package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.exception.MinioStorageException;
import com.neighbor.eventmosaic.collector.service.MinioStorageService;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Реализация сервиса {@link MinioStorageService} для взаимодействия с хранилищем MinIO.
 * Использует официальный MinIO Java SDK.
 */
@Slf4j
@Service
public class MinioStorageServiceImpl implements MinioStorageService {

    @Value("${storage.minio.endpoint}")
    private String endpoint;

    @Value("${storage.minio.access-key}")
    private String accessKey;

    @Value("${storage.minio.secret-key}")
    private String secretKey;

    @Value("${storage.minio.bucket}")
    private String bucketName;

    private MinioClient minioClient;

    /**
     * Инициализирует MinioClient после внедрения зависимостей Spring.
     * Выполняет проверку доступности бакета и создает его при необходимости.
     * В случае неустранимой ошибки инициализации выбрасывает {@link IllegalStateException},
     * предотвращая запуск приложения с нерабочим компонентом.
     */
    @PostConstruct
    private void initializeMinioClient() {
        log.info("Инициализация MinIO клиента. Endpoint: '{}', Bucket: '{}'", endpoint, bucketName);
        try {
            minioClient = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey)
                    .build();

            checkAndCreateBucket(); // Проверяем и создаем бакет, если он отсутствует

            log.info("MinIO клиент успешно инициализирован и подключен к бакету '{}'", bucketName);

        } catch (Exception e) {
            log.error("Критическая ошибка: Не удалось инициализировать MinIO клиент или проверить бакет '{}'. Причина: {}",
                    bucketName, e.getMessage(), e);
            throw new IllegalStateException("Сбой инициализации MinIO клиента", e);
        }
    }

    /**
     * Загружает локальный файл в хранилище MinIO.
     * Автоматически определяет размер и Content-Type файла.
     *
     * @param objectName Имя, под которым объект будет сохранен в MinIO (включая путь, если нужно, например, "subdir/my-file.csv").
     * @param filePath   Путь к локальному файлу для загрузки.
     * @return Публичный или внутренний URL загруженного объекта в MinIO. Доступность зависит от конфигурации бакета.
     * @throws MinioStorageException если произошла ошибка при чтении файла или загрузке в MinIO.
     */
    @Override
    public String uploadFile(String objectName,
                             Path filePath) {
        log.debug("Начало загрузки файла '{}' как объект '{}' в бакет '{}'", filePath, objectName, bucketName);

        try (InputStream inputStream = Files.newInputStream(filePath)) {
            long fileSize = Files.size(filePath);
            String contentType = determineContentType(filePath);

            return uploadStreamInternal(objectName, inputStream, fileSize, contentType);

        } catch (IOException e) {
            log.error("Ошибка чтения локального файла '{}' перед загрузкой в MinIO: {}", filePath, e.getMessage(), e);
            throw new MinioStorageException("Ошибка чтения файла: " + filePath, e);
        }
    }

    /**
     * Получает постоянный URL для доступа к объекту в MinIO.
     * Метод формирует стандартный URL вида: {@code <endpoint>/<bucket>/<objectName>}.
     * Фактическая доступность URL зависит от политики доступа, настроенной для бакета в MinIO.
     *
     * @param objectName Имя объекта в MinIO.
     * @return Строка, представляющая URL объекта.
     * @throws MinioStorageException если произошла ошибка
     */
    @Override
    public String getObjectUrl(String objectName) {
        log.debug("Формирование URL для объекта '{}' в бакете '{}' (endpoint: '{}')", objectName, bucketName, endpoint);

        // Убедимся, что endpoint не заканчивается на '/' для корректной конкатенации
        String cleanEndpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;

        // Формируем простой URL. Для доступа по этому URL бакет должен быть публичным
        // или доступ должен осуществляться из внутренней сети/через прокси.
        String url = String.format("%s/%s/%s", cleanEndpoint, bucketName, objectName);
        log.debug("Сформирован URL: {}", url);
        return url;
    }

    /**
     * Удаляет объект из хранилища MinIO.
     *
     * @param objectName Имя объекта для удаления.
     * @throws MinioStorageException если произошла ошибка при удалении объекта.
     */
    @Override
    public void deleteObject(String objectName) {
        log.debug("Начало удаления объекта '{}' из бакета '{}'", objectName, bucketName);

        try {
            minioClient.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .build());
            log.info("Объект '{}' успешно удален из бакета '{}'", objectName, bucketName);

        } catch (Exception e) {
            log.error("Ошибка при удалении объекта '{}' из MinIO бакета '{}': {}", objectName, bucketName, e.getMessage(), e);
            throw new MinioStorageException("Ошибка при удалении объекта '" + objectName + "' из MinIO", e);
        }
    }


    /**
     * Проверяет существование бакета и создает его, если он отсутствует.
     * Этот метод вызывается при инициализации сервиса.
     *
     * @throws MinioStorageException если произошла ошибка при взаимодействии с MinIO API.
     */
    private void checkAndCreateBucket() {
        try {
            boolean found = minioClient.bucketExists(
                    BucketExistsArgs.builder()
                            .bucket(bucketName)
                            .build()
            );

            if (!found) {
                log.warn("Бакет '{}' не найден в MinIO по адресу '{}'. Попытка создания...", bucketName, endpoint);
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                log.info("Бакет '{}' успешно создан.", bucketName);

            } else {
                log.debug("Бакет '{}' уже существует.", bucketName);
            }
        } catch (Exception e) {
            throw new MinioStorageException("Ошибка при проверке или создании бакета " + bucketName, e);
        }
    }

    /**
     * Внутренний метод для выполнения загрузки потока данных в MinIO.
     *
     * @param objectName  Имя объекта в MinIO.
     * @param inputStream Поток данных (будет закрыт вызывающим кодом или try-with-resources).
     * @param size        Размер данных (если известен, иначе -1).
     * @param contentType MIME-тип содержимого.
     * @return URL загруженного объекта.
     * @throws MinioStorageException при ошибке во время загрузки.
     */
    private String uploadStreamInternal(String objectName,
                                        InputStream inputStream,
                                        long size,
                                        String contentType) {
        try {
            PutObjectArgs args = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(inputStream, size, -1) // size = -1 (неизвестен), partSize = -1 (авто)
                    .contentType(contentType)
                    .build();

            ObjectWriteResponse response = minioClient.putObject(args);
            log.info("Объект '{}' успешно загружен в бакет '{}'. VersionId: {}, ETag: {}",
                    response.object(),
                    response.bucket(),
                    response.versionId(),
                    response.etag());

            return getObjectUrl(objectName);

        } catch (Exception e) {
            log.error("Ошибка при загрузке объекта '{}' в MinIO бакет '{}': {}", objectName, bucketName, e.getMessage(), e);
            throw new MinioStorageException("Ошибка при загрузке объекта " + objectName + " в MinIO", e);
        }
    }

    /**
     * Пытается определить MIME-тип файла по его расширению.
     * Возвращает "application/octet-stream", если тип не удалось определить.
     *
     * @param filePath Путь к файлу.
     * @return Определенный MIME-тип или тип по умолчанию.
     */
    private String determineContentType(Path filePath) {
        try {
            String contentType = Files.probeContentType(filePath);
            if (contentType == null) {
                log.warn("Не удалось определить Content-Type для файла '{}'. Используется тип по умолчанию 'application/octet-stream'.", filePath);
                return "application/octet-stream";
            }
            log.debug("Определен Content-Type '{}' для файла '{}'", contentType, filePath);
            return contentType;

        } catch (IOException e) {
            log.warn("Ошибка при определении Content-Type для файла '{}': {}. Используется тип по умолчанию 'application/octet-stream'.",
                    filePath, e.getMessage());
            return "application/octet-stream";
        }
    }
}