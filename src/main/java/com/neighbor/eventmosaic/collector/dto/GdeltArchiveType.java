package com.neighbor.eventmosaic.collector.dto;

import com.neighbor.eventmosaic.collector.config.KafkaTopicProperties;
import lombok.Getter;

import java.util.Arrays;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Типы архивов GDELT.
 * Каждый тип архива соответствует определенному шаблону имени файла
 * и предоставляет метод для получения имени топика Kafka,
 * в который будут отправляться данные из этого архива.
 */
@Getter
public enum GdeltArchiveType {

    TRANSLATION_EXPORT("translation\\.export\\.CSV\\.zip$", KafkaTopicProperties::getCollectorEvent),
    TRANSLATION_MENTIONS("translation\\.mentions\\.CSV\\.zip$", KafkaTopicProperties::getCollectorMention);

    private final Pattern pattern;
    private final Function<KafkaTopicProperties, String> topicExtractor;

    GdeltArchiveType(String regex, Function<KafkaTopicProperties, String> topicExtractor) {
        this.pattern = Pattern.compile(regex);
        this.topicExtractor = topicExtractor;
    }

    /**
     * Получает имя топика Kafka для этого типа архива.
     *
     * @param properties конфигурация топиков
     * @return имя топика Kafka
     */
    public String getTopicName(KafkaTopicProperties properties) {
        return topicExtractor.apply(properties);
    }

    /**
     * Определяет тип архива по имени файла.
     *
     * @param fileName имя файла архива
     * @return тип архива или null, если тип не определен
     */
    public static GdeltArchiveType fromFileName(String fileName) {
        return Arrays.stream(values())
                .filter(type -> type.pattern.matcher(fileName).find())
                .findFirst()
                .orElse(null);
    }
}
