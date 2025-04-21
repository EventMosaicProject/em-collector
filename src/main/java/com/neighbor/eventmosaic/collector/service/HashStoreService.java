package com.neighbor.eventmosaic.collector.service;

/**
 * Сервис для работы с хранилищем хешей архивов.
 * Предоставляет методы для проверки и сохранения хешей архивов.
 */
public interface HashStoreService {

    /**
     * Получает сохраненный ранее хеш для указанного архива.
     *
     * @param archiveName Имя архива
     * @return Хеш архива или null, если архив отсутствует в хранилище
     */
    String getStoredHash(String archiveName);

    /**
     * Сохраняет хеш архива в хранилище.
     *
     * @param archiveName Имя архива
     * @param hash        Хеш архива для сохранения
     */
    void storeHash(String archiveName, String hash);

    /**
     * Проверяет, является ли архив новым или измененным.
     *
     * @param archiveName Имя архива
     * @param currentHash Текущий хеш архива
     * @return true, если архив новый или его хеш изменился, иначе false
     */
    boolean isNewOrChanged(String archiveName, String currentHash);
}