package com.neighbor.eventmosaic.collector.scheduler;

import com.neighbor.eventmosaic.collector.dto.ExtractedFileInfo;
import com.neighbor.eventmosaic.collector.resolver.GdeltTopicResolver;
import com.neighbor.eventmosaic.collector.service.FileSendStatusService;
import com.neighbor.eventmosaic.collector.service.KafkaMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Планировщик повторной отправки файлов, которые не были успешно отправлены в Kafka.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FileSendRetryScheduler {

    private final KafkaMessageService kafkaMessageService;
    private final FileSendStatusService sendStatusService;
    private final GdeltTopicResolver topicResolver;

    /**
     * Запускается по расписанию для повторной отправки файлов.
     * Что делает:
     * 1. Получает список файлов для повторной отправки из Redis.
     * 2. Проверяет, существует ли файл.
     * 3. Определяет топик Kafka, в который будет отправлен файл.
     * 4. Повторно отправляет файл в Kafka.
     */
    @Scheduled(fixedDelayString = "${gdelt.retry.interval}")
    public void retryPendingFiles() {
        log.info("Запуск задачи повторной отправки файлов...");

        List<ExtractedFileInfo> pendingFiles = sendStatusService.getPendingFiles();
        log.info("Найдено файлов для повторной отправки: {}", pendingFiles.size());

        pendingFiles.forEach(fileInfo -> {
            Path filePath = Paths.get(fileInfo.getFilePath());

            if (!Files.exists(filePath)) {
                log.warn("Файл не существует, пропускаем: {}", fileInfo.getFilePath());
                return;
            }

            String topic = topicResolver.resolveTopic(fileInfo.getArchiveFileName());
            log.info("Повторная отправка файла {} в топик {}", filePath, topic);

            kafkaMessageService.sendFilePathToKafka(topic, filePath);
        });
    }
}
