package com.neighbor.eventmosaic.collector.listener;

import com.neighbor.eventmosaic.collector.event.ArchiveExtractedEvent;
import com.neighbor.eventmosaic.collector.resolver.GdeltTopicResolver;
import com.neighbor.eventmosaic.collector.service.FileSendStatusService;
import com.neighbor.eventmosaic.collector.service.KafkaMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Обработчик событий успешной распаковки архивов.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ArchiveExtractedEventListener {

    private final KafkaMessageService kafkaMessageService;
    private final GdeltTopicResolver topicResolver;
    private final FileSendStatusService sendStatusService;

    /**
     * Асинхронно обрабатывает событие успешной распаковки архива.
     * Определяет топик Kafka, в который будет отправлен файл.
     * Регистрирует файл в Redis и отправляет его в Kafka.
     *
     * @param event событие
     */
    @Async
    @EventListener
    public void handleArchiveExtractedEvent(ArchiveExtractedEvent event) {
        log.info("Получено событие о распаковке архива: {}", event.archiveInfo().fileName());

        String topic = topicResolver.resolveTopic(event.archiveInfo().fileName());
        String archiveFileName = event.archiveInfo().fileName();

        event.extractedFiles().forEach(filePath -> {
            sendStatusService.registerFile(archiveFileName, filePath);
            kafkaMessageService.sendUrlToKafka(topic, filePath);
        });

        log.info("Обработка события завершена.");
    }
}