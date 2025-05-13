package com.neighbor.eventmosaic.collector.event;

import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;

import java.util.List;

/**
 * Событие успешного извлечения файлов из архива.
 * Содержит информацию об архиве и список URL к извлеченным файлам.
 */
public record ArchiveExtractedEvent(

        // Информация об архиве GDELT
        GdeltArchiveInfo archiveInfo,

        // Список URL к извлеченным файлам (пока всегда 1 файл)
        List<String> extractedFiles
) {
}