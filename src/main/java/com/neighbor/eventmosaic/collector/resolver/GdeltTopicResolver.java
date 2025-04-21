package com.neighbor.eventmosaic.collector.resolver;

import com.neighbor.eventmosaic.collector.config.KafkaTopicProperties;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Решает, в какой топик Kafka отправлять файл.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GdeltTopicResolver {

    private final KafkaTopicProperties topicProperties;

    /**
     * Определяет топик Kafka для отправки файла.
     *
     * @param archiveFileName имя архива
     * @return топик Kafka
     */
    public String resolveTopic(String archiveFileName) {
        GdeltArchiveType archiveType = GdeltArchiveType.fromFileName(archiveFileName);
        if (archiveType == null) {
            log.error("Не удалось определить тип архива для файла: {}", archiveFileName);
            throw new IllegalArgumentException("Неизвестный тип архива: " + archiveFileName);
        }

        return archiveType.getTopicName(topicProperties);
    }
}
