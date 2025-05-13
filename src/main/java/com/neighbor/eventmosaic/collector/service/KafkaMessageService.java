package com.neighbor.eventmosaic.collector.service;

import java.util.concurrent.CompletableFuture;

/**
 * Сервис для отправки сообщений (URL файлов) в Kafka.
 * Обрабатывает отправку URL и обновляет статус файла в сервисе отслеживания.
 */
public interface KafkaMessageService {

    /**
     * Отправляет URL файла в указанный топик Kafka.
     * После успешной отправки файл помечается как отправленный в сервисе отслеживания.
     */
    CompletableFuture<Void> sendUrlToKafka(String topic, String fileUrl);
}