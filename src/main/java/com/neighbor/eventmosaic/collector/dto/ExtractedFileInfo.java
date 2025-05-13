package com.neighbor.eventmosaic.collector.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Информация о распакованном файле для отслеживания отправки в Kafka.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExtractedFileInfo {

    /**
     * Имя архива, из которого был извлечен файл.
     * Используется для определения топика через {@link com.neighbor.eventmosaic.collector.resolver.GdeltTopicResolver}.
     */
    private String archiveFileName;

    /**
     * URL файла в хранилище.
     */
    private String fileUrl;

    /**
     * Статус отправки файла.
     */
    private boolean isSent;
}
