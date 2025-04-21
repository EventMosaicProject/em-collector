package com.neighbor.eventmosaic.collector.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Основная конфигурация приложения.
 * Включает поддержку Feign-клиентов, асинхронное выполнение и планирование задач.
 */
@Slf4j
@Configuration
@EnableAsync
@EnableScheduling
@EnableFeignClients(basePackages = "com.neighbor.eventmosaic.collector.client")
@EnableConfigurationProperties(KafkaTopicProperties.class)
public class AppConfig {

    /**
     * Создает виртуальный пул потоков для асинхронного выполнения задач.
     * Использует виртуальные потоки Java для повышения производительности.
     *
     * @return Executor для асинхронных задач
     */
    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        return Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual()
                        .name("GdeltTask-", 0)
                        .uncaughtExceptionHandler((thread, ex) -> log.error("Необработанное исключение в виртуальном потоке {}: {}",
                                thread.getName(), ex.getMessage(), ex))
                        .factory());
    }
}