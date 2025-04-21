package com.neighbor.eventmosaic.collector.controller;

import com.neighbor.eventmosaic.collector.service.GdeltService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST-контроллер для управления процессом обработки данных GDELT.
 * Предоставляет эндпоинты для ручного запуска задач.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/gdelt")
@RequiredArgsConstructor
public class GdeltController {

    private final GdeltService gdeltService;

    /**
     * Эндпоинт для ручного запуска процесса обработки архивов GDELT.
     *
     * @return Информация о запуске процесса
     */
    @PostMapping("/process")
    public ResponseEntity<String> processGdeltArchives() {
        log.info("Получен запрос на ручную обработку архивов GDELT");

        gdeltService.processLatestArchives()
                .exceptionally(ex -> {
                    log.error("Ошибка при ручной обработке архивов: {}", ex.getMessage(), ex);
                    return null;
                });

        return ResponseEntity.accepted().body("Процесс обработки архивов GDELT запущен");
    }
}