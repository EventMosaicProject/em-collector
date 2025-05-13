package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.service.FileSendStatusService;
import com.neighbor.eventmosaic.collector.service.KafkaMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Реализация сервиса для отправки сообщений (URL файлов) в Kafka.
 * Обрабатывает отправку URL и обновляет статус файла в сервисе отслеживания.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaMessageServiceImpl implements KafkaMessageService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final FileSendStatusService fileSendStatusService;

    /**
     * Отправляет URL файла в указанный топик Kafka.
     * После успешной отправки файл помечается как отправленный в сервисе отслеживания.
     * Метод возвращает CompletableFuture, который может быть использован для асинхронного отслеживания
     * результата отправки.
     *
     * @param topic   Топик Kafka для отправки
     * @param fileUrl URL файла, который будет отправлен
     * @return CompletableFuture, который завершается, когда сообщение подтверждено Kafka
     */
    @Override
    public CompletableFuture<Void> sendFilePathToKafka(String topic,
                                                       String fileUrl) {

        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        try {
            log.info("Отправка URL файла в топик Kafka '{}': {}", topic, fileUrl);

            kafkaTemplate.send(topic, fileUrl)
                    .whenComplete((sendResult, ex) -> {
                        if (ex == null) {
                            // Успешная отправка
                            log.info("Сообщение успешно отправлено в Kafka. Топик: {}, Смещение: {}, URL: {}",
                                    sendResult.getRecordMetadata().topic(),
                                    sendResult.getRecordMetadata().offset(),
                                    fileUrl);

                            // Обновляем статус файла на 'отправлен'
                            boolean marked = fileSendStatusService.markAsSent(fileUrl);
                            if (!marked) {
                                log.warn("Сообщение для URL '{}' отправлено в Kafka, но не удалось обновить статус в Redis.", fileUrl);
                            }
                            resultFuture.complete(null);
                        } else {
                            log.error("Ошибка при отправке сообщения в Kafka. Топик: {}, URL: {}, Ошибка: {}",
                                    topic, fileUrl, ex.getMessage(), ex);
                            resultFuture.completeExceptionally(ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Непредвиденная ошибка при инициации отправки в Kafka. Топик: {}, URL: {}, Ошибка: {}",
                    topic, fileUrl, e.getMessage(), e);
            resultFuture.completeExceptionally(e);
        }

        return resultFuture;
    }
}