package com.neighbor.eventmosaic.collector.event;

import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;

import java.nio.file.Path;
import java.util.List;

/**
 * Событие успешного извлечения файлов из архива.
 * Содержит информацию об архиве и список путей к извлеченным файлам.
 */
public record ArchiveExtractedEvent(

        // Информация об архиве GDELT
        GdeltArchiveInfo archiveInfo,

        // Список путей к извлеченным файлам (пока всегда 1 файл)
        List<Path> extractedFiles
) {
}