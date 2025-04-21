package com.neighbor.eventmosaic.collector.service;

import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveProcessResult;

import java.util.concurrent.CompletableFuture;

/**
 * Сервис для обработки архивов GDELT.
 * Отвечает за скачивание, проверку и распаковку архивов.
 */
public interface ArchiveService {

    /**
     * Асинхронно обрабатывает архив GDELT.
     * Включает загрузку, проверку хеша, распаковку и удаление исходного архива.
     *
     * @param archive Информация об архиве для обработки
     * @return CompletableFuture с результатом обработки архива
     */
    CompletableFuture<GdeltArchiveProcessResult> processArchiveAsync(GdeltArchiveInfo archive);
}