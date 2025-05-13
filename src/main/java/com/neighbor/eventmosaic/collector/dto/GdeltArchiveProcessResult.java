package com.neighbor.eventmosaic.collector.dto;

import java.util.Collections;
import java.util.List;

/**
 * Результат обработки архива GDELT.
 * Содержит информацию о результате обработки, включая статус,
 * обрабатываемый архив, URL к извлеченным файлам.
 */
public record GdeltArchiveProcessResult(
        // Флаг успешности обработки
        boolean isSuccess,

        // Информация об обработанном архиве
        GdeltArchiveInfo archive,

        // Список URL к извлеченным файлам
        List<String> extractedFiles,

        // Сообщение об ошибке (если есть)
        String errorMessage
) {

    // Для успешного результата
    public static GdeltArchiveProcessResult success(GdeltArchiveInfo archive,
                                                    List<String> extractedFiles) {
        return new GdeltArchiveProcessResult(
                true,
                archive,
                extractedFiles,
                null);
    }

    // Для неудачного результата
    public static GdeltArchiveProcessResult failure(GdeltArchiveInfo archive,
                                                    String errorMessage) {
        return new GdeltArchiveProcessResult(
                false,
                archive,
                Collections.emptyList(),
                errorMessage);
    }
}