package com.neighbor.eventmosaic.collector.service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Сервис для работы с файловой системой.
 * Предоставляет методы для загрузки файлов, расчета хешей,
 * работы с архивами и создания директорий.
 */
public interface FileSystemService {

    /**
     * Загружает файл по URL и сохраняет его по указанному пути.
     *
     * @param url        URL файла для загрузки
     * @param targetPath Путь для сохранения файла
     * @return Путь к загруженному файлу
     * @throws IOException при ошибках ввода-вывода
     */
    Path downloadFile(String url, String targetPath) throws IOException;

    /**
     * Вычисляет MD5-хеш для указанного файла.
     *
     * @param filePath Путь к файлу
     * @return MD5-хеш файла
     * @throws IOException при ошибках ввода-вывода
     */
    String calculateMd5(Path filePath) throws IOException;

    /**
     * Распаковывает ZIP-архив в указанную директорию.
     *
     * @param zipFile   Путь к ZIP-архиву
     * @param targetDir Директория для распаковки
     * @return Список путей к распакованным файлам
     * @throws IOException при ошибках ввода-вывода
     */
    List<Path> extractZipFile(Path zipFile, Path targetDir) throws IOException;

    /**
     * Создает директорию, если она не существует.
     *
     * @param directory Путь к директории
     * @return Путь к созданной или существующей директории
     * @throws IOException при ошибках ввода-вывода
     */
    Path checkAndCreateDirectory(String directory) throws IOException;
}