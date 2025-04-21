package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.service.FileSendStatusService;
import com.neighbor.eventmosaic.collector.service.KafkaMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * Реализация сервиса для отправки сообщений в Kafka.
 * Обрабатывает отправку путей к файлам и обновляет их статус в сервисе отслеживания.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaMessageServiceImpl implements KafkaMessageService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final FileSendStatusService fileSendStatusService;

    /**
     * Отправляет абсолютный путь к файлу в указанный топик Kafka.
     * После успешной отправки файл помечается как отправленный в сервисе отслеживания.
     * Метод возвращает CompletableFuture, который может быть использован для асинхронного отслеживания
     * результата отправки.
     *
     * @param topic    Топик Kafka для отправки
     * @param filePath Путь к файлу, который будет отправлен
     * @return CompletableFuture, который завершается, когда сообщение подтверждено Kafka
     */
    @Override
    public CompletableFuture<Void> sendFilePathToKafka(String topic, Path filePath) {
        String message = filePath.toAbsolutePath().toString();
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        try {
            log.debug("Отправка сообщения в топик {}: {}", topic, message);

            kafkaTemplate.send(topic, message)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            // Успешная отправка - обновляем статус
                            fileSendStatusService.markAsSent(filePath);
                            log.debug("Сообщение успешно отправлено в топик {}: {}", topic, message);
                            resultFuture.complete(null);
                        } else {
                            log.error("Ошибка при отправке сообщения в топик {}: {}", topic, ex.getMessage(), ex);
                            resultFuture.completeExceptionally(ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Ошибка при попытке отправки в Kafka: {}", e.getMessage(), e);
            resultFuture.completeExceptionally(e);
        }

        return resultFuture;
    }
}