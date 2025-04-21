package com.neighbor.eventmosaic.collector.dto;

/**
 * Модель данных, представляющая информацию об архиве GDELT.
 * Содержит метаданные архива, включая имя файла, URL, хеш и размер.
 */
public record GdeltArchiveInfo(
        // Имя файла архива
        String fileName,

        // Полный URL для загрузки архива
        String url,

        // MD5-хеш архива для проверки целостности
        String hash,

        // Размер архива в байтах
        long size
) {
}
