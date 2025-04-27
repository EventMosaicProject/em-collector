package com.neighbor.eventmosaic.collector.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Свойства топиков Kafka.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "kafka.topic.producer")
public class KafkaTopicProperties {

    private String collectorEvent;
    private String collectorMention;
}
