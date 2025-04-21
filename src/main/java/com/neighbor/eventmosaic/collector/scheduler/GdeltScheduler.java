package com.neighbor.eventmosaic.collector.scheduler;

import com.neighbor.eventmosaic.collector.service.GdeltService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Планировщик для автоматического запуска задач обработки данных GDELT.
 * Выполняет периодическую проверку новых архивов.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GdeltScheduler {

    private final GdeltService gdeltService;

    /**
     * Выполняет проверку и обработку новых архивов GDELT по расписанию.
     * Запускается каждую минуту.
     */
    @Scheduled(fixedDelayString = "${gdelt.check.interval}")
    public void scheduledFetchArchives() {
        log.info("Запуск запланированной задачи проверки архивов GDELT");
        gdeltService.processLatestArchives()
                .exceptionally(ex -> {
                    log.error("Ошибка при выполнении запланированной задачи: {}", ex.getMessage(), ex);
                    return null;
                });
    }
}