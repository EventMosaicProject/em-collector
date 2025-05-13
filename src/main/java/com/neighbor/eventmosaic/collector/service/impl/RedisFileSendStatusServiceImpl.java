package com.neighbor.eventmosaic.collector.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neighbor.eventmosaic.collector.dto.ExtractedFileInfo;
import com.neighbor.eventmosaic.collector.service.FileSendStatusService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Реализация сервиса статуса отправки сообщений в топик Kafka на основе Redis.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisFileSendStatusServiceImpl implements FileSendStatusService {

    private static final String FILE_INFO_PREFIX = "gdelt:file:info:";
    private static final Duration TTL = Duration.ofHours(1); // Время хранения статусов

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Регистрирует файл в Redis.
     * Когда файл зарегистрирован, он добавляется в список ожидающих отправки в топик Kafka.
     * По-дефолту статус - не отправлен.
     *
     * @param archiveFileName имя архива
     * @param fileUrl         путь к файлу
     * @return true, если зарегистрирован, false - если нет
     */
    @Override
    public boolean registerFile(String archiveFileName, String fileUrl) {

        ExtractedFileInfo extractedFileInfo = ExtractedFileInfo.builder()
                .archiveFileName(archiveFileName)
                .fileUrl(fileUrl)
                .isSent(false) // начальный статус - не отправлен
                .build();

        return saveFileInfo(fileUrl, extractedFileInfo);
    }

    /**
     * Отмечает файл как отправленный в топик Kafka.
     *
     * @param fileUrl путь к файлу
     * @return true, если отправлен, false - если нет
     */
    @Override
    public boolean markAsSent(String fileUrl) {
        ExtractedFileInfo fileInfo = getFileInfo(fileUrl);
        if (fileInfo == null) {
            log.warn("Попытка отметить как доставленный незарегистрированный файл: {}", fileUrl);
            return false;
        }

        fileInfo.setSent(true);
        return saveFileInfo(fileUrl, fileInfo);
    }

    /**
     * Получает информацию о файле из Redis.
     *
     * @param fileUrl путь к файлу
     * @return информация о файле
     */
    @Override
    public ExtractedFileInfo getFileInfo(String fileUrl) {
        String key = buildFileInfoKey(fileUrl);
        try {
            String json = redisTemplate.opsForValue().get(key);
            if (json == null) {
                return null;
            }
            return objectMapper.readValue(json, ExtractedFileInfo.class);
        } catch (Exception e) {
            log.error("Ошибка при получении информации о файле из Redis. URL: {}, Ключ: {}, Ошибка: {}",
                    fileUrl, key, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Получает список ожидающих отправки файлов из Redis или пустой список.
     *
     * @return список ожидающих отправки файлов
     */
    // TODO: Оптимизировать метод
    @Override
    public List<ExtractedFileInfo> getPendingFiles() {
        log.debug("Запрос списка неотправленных файлов из Redis...");
        List<ExtractedFileInfo> pendingFiles = new ArrayList<>();
        try {
            // Получаем все ключи с префиксом
            Set<String> keys = redisTemplate.keys(FILE_INFO_PREFIX + "*");
            if (keys.isEmpty()) {
                return pendingFiles;
            }

            // Проверяем каждый файл
            for (String key : keys) {
                String json = redisTemplate.opsForValue().get(key);
                if (json != null) {
                    ExtractedFileInfo fileInfo = objectMapper.readValue(json, ExtractedFileInfo.class);
                    if (!fileInfo.isSent()) {
                        pendingFiles.add(fileInfo);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Ошибка при получении списка ожидающих файлов: {}", e.getMessage(), e);
        }
        return pendingFiles;
    }

    /**
     * Сохраняет информацию о файле в Redis.
     * Устанавливает время жизни ключа.
     *
     * @param fileUrl  путь к файлу
     * @param fileInfo информация о файле
     * @return true, если сохранено, false - если нет
     */
    private boolean saveFileInfo(String fileUrl, ExtractedFileInfo fileInfo) {
        String key = buildFileInfoKey(fileUrl);
        try {
            String json = objectMapper.writeValueAsString(fileInfo);
            redisTemplate.opsForValue().set(key, json, TTL);
            return true;

        } catch (JsonProcessingException e) {
            log.error("Ошибка сериализации информации о файле. URL: {}, Данные: {}, Ошибка: {}",
                    fileUrl, fileInfo, e.getMessage(), e);
            return false;

        } catch (Exception e) {
            log.error("Ошибка при сохранении информации о файле в Redis. Ключ: {}, Ошибка: {}",
                    key, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Строит ключ Redis для хранения информации о файле на основе его URL.
     *
     * @param fileUrl URL файла.
     * @return Строка, представляющая ключ Redis.
     */
    private String buildFileInfoKey(String fileUrl) {
        return FILE_INFO_PREFIX + fileUrl;
    }
}
