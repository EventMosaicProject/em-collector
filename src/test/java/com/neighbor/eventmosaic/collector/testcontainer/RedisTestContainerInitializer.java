package com.neighbor.eventmosaic.collector.testcontainer;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Инициализатор тестового контейнера Redis.
 * Используется для тестирования приложения с Redis.
 */
public interface RedisTestContainerInitializer {

    String DOCKER_IMAGE_NAME = "redis:7-alpine";
    String SPRING_DATA_REDIS_HOST = "spring.data.redis.host";
    String SPRING_DATA_REDIS_PORT = "spring.data.redis.port";
    String SPRING_DATA_REDIS_PASSWORD = "spring.data.redis.password";

    @Container
    GenericContainer<?> REDIS_CONTAINER = new GenericContainer<>(DOCKER_IMAGE_NAME)
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void registerRedisProperties(DynamicPropertyRegistry registry) {
        registry.add(SPRING_DATA_REDIS_HOST, REDIS_CONTAINER::getHost);
        registry.add(SPRING_DATA_REDIS_PORT, REDIS_CONTAINER::getFirstMappedPort);
        registry.add(SPRING_DATA_REDIS_PASSWORD, () -> "");
    }
}
