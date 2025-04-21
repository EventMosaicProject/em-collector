package com.neighbor.eventmosaic.collector.service;

import java.util.concurrent.CompletableFuture;

/**
 * Сервис для работы с данными GDELT.
 * Предоставляет методы для получения и обработки архивов GDELT.
 */
public interface GdeltService {

    /**
     * Запускает процесс обработки последних доступных архивов GDELT.
     * Включает получение списка архивов, фильтрацию новых/измененных
     * и их асинхронную обработку.
     *
     * @return CompletableFuture, который завершается, когда обработка всех архивов завершена
     */
    CompletableFuture<Void> processLatestArchives();
}