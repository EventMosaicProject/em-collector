package com.neighbor.eventmosaic.collector.config;

import feign.Logger;
import feign.RetryableException;
import feign.Retryer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Конфигурация Feign-клиентов.
 * Настраивает механизм повторных попыток и другие параметры клиентов.
 */
@Slf4j
@Configuration
public class FeignClientConfig {

    @Value("${gdelt.client.retry.period:1000}")
    private long retryPeriod;       // Период ожидания между попытками (в мс)

    @Value("${gdelt.client.retry.max-period:5000}")
    private long maxRetryPeriod;    // Максимальный период ожидания между попытками (в мс)

    @Value("${gdelt.client.retry.max-attempts:3}")
    private int maxAttempts;        // Максимальное количество попыток

    /**
     * Настройка стратегии повторных попыток для Feign-клиентов.
     * Использует задержку между попытками.
     *
     * @return Настроенный экземпляр Retryer
     */
    @Bean
    public Retryer feignRetryer() {
        return new Retryer.Default(retryPeriod, maxRetryPeriod, maxAttempts);
    }

    /**
     * Настройка обработчика ошибок для логирования неудачных попыток.
     */
    @Bean
    public Logger feignLogger() {
        return new feign.Logger() {
            @Override
            protected void log(String configKey, String format, Object... args) {
                if (args.length > 0 && args[0] instanceof RetryableException) {
                    log.warn("Попытка запроса к {} не удалась: {}. Будет выполнена повторная попытка.",
                            configKey, args[0]);
                }
            }
        };
    }
} 