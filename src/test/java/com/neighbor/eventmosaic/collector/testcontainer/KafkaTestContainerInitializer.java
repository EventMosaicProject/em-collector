package com.neighbor.eventmosaic.collector.testcontainer;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Инициализатор тестового контейнера Kafka.
 * Используется для тестирования приложения с Kafka.
 */
public interface KafkaTestContainerInitializer {

    String DOCKER_IMAGE_NAME = "confluentinc/cp-kafka:7.4.0";
    String SPRING_KAFKA_BOOTSTRAP_SERVERS = "spring.kafka.bootstrap-servers";

    @Container
    ConfluentKafkaContainer  KAFKA_CONTAINER = new ConfluentKafkaContainer(DockerImageName.parse(DOCKER_IMAGE_NAME));

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        KAFKA_CONTAINER.start();
        registry.add(SPRING_KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONTAINER::getBootstrapServers);
    }
}
