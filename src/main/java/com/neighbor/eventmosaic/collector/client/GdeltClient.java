package com.neighbor.eventmosaic.collector.client;

import com.neighbor.eventmosaic.collector.config.FeignClientConfig;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Feign-клиент для взаимодействия с API GDELT.
 * Обеспечивает доступ к данным о последних архивах переводов.
 */
@FeignClient(
        name = "${gdelt.client.name}",
        url = "${gdelt.api.base-url}",
        configuration = FeignClientConfig.class
)
public interface GdeltClient {

    /**
     * Получает информацию о последних доступных архивах переводов.
     *
     * @return Строка с информацией об архивах в формате:
     * размер хеш URL для каждого архива на отдельной строке
     */
    @GetMapping("${gdelt.api.translation-updates-path}")
    String getLatestArchivesList();
}