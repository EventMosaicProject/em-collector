package com.neighbor.eventmosaic.collector.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Свойства топиков Kafka.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "kafka.topic")
public class KafkaTopicProperties {

    private String translationExport;
    private String translationMentions;
}
