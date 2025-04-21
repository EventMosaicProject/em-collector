package com.neighbor.eventmosaic.collector.service;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * Сервис для отправки сообщений в Kafka.
 * Предоставляет методы для отправки путей к файлам в соответствующие топики.
 */
public interface KafkaMessageService {

    /**
     * Отправляет абсолютный путь к файлу в указанный топик Kafka.
     * После успешной отправки файл помечается как отправленный в сервисе отслеживания.
     *
     * @param topic    Топик Kafka для отправки
     * @param filePath Путь к файлу, который будет отправлен
     * @return CompletableFuture, который завершается, когда сообщение подтверждено Kafka
     */
    CompletableFuture<Void> sendFilePathToKafka(String topic, Path filePath);
}