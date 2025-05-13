package com.neighbor.eventmosaic.collector.scheduler;

import com.neighbor.eventmosaic.collector.dto.ExtractedFileInfo;
import com.neighbor.eventmosaic.collector.resolver.GdeltTopicResolver;
import com.neighbor.eventmosaic.collector.service.FileSendStatusService;
import com.neighbor.eventmosaic.collector.service.KafkaMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Планировщик повторной отправки URL файлов, которые не были успешно отправлены в Kafka.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FileSendRetryScheduler {

    private final KafkaMessageService kafkaMessageService;
    private final FileSendStatusService sendStatusService;
    private final GdeltTopicResolver topicResolver;

    /**
     * Запускается по расписанию для повторной отправки URL файлов в Kafka.
     * 1. Получает из {@link FileSendStatusService} список файлов (по их URL), ожидающих отправки.
     * 2. Для каждого файла определяет топик Kafka.
     * 3. Повторно инициирует отправку URL файла в Kafka через {@link KafkaMessageService}.
     */
    @Scheduled(fixedDelayString = "${gdelt.retry.interval}")
    public void retryPendingFiles() {
        log.info("Запуск задачи повторной отправки файлов...");

        List<ExtractedFileInfo> pendingFiles = sendStatusService.getPendingFiles();
        log.info("Найдено файлов для повторной отправки: {}", pendingFiles.size());

        pendingFiles.forEach(fileInfo -> {
            String fileUrl = fileInfo.getFileUrl();

            String topic = topicResolver.resolveTopic(fileInfo.getArchiveFileName());
            log.info("Повторная отправка файла {} в топик {}", fileUrl, topic);

            kafkaMessageService.sendUrlToKafka(topic, fileUrl);
        });
    }
}
