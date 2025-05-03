package com.neighbor.eventmosaic.collector.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neighbor.eventmosaic.collector.dto.ExtractedFileInfo;
import com.neighbor.eventmosaic.collector.service.FileSendStatusService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
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
     * @param filePath        путь к файлу
     * @return true, если зарегистрирован, false - если нет
     */
    @Override
    public boolean registerFile(String archiveFileName, Path filePath) {

        ExtractedFileInfo extractedFileInfo = ExtractedFileInfo.builder()
                .archiveFileName(archiveFileName)
                .filePath(filePath.toAbsolutePath().toString())
                .isSent(false) // начальный статус - не отправлен
                .build();

        return saveFileInfo(filePath, extractedFileInfo);
    }

    /**
     * Отмечает файл как отправленный в топик Kafka.
     *
     * @param filePath путь к файлу
     * @return true, если отправлен, false - если нет
     */
    @Override
    public boolean markAsSent(Path filePath) {
        ExtractedFileInfo fileInfo = getFileInfo(filePath);
        if (fileInfo == null) {
            log.warn("Попытка отметить как доставленный незарегистрированный файл: {}", filePath);
            return false;
        }

        fileInfo.setSent(true);
        return saveFileInfo(filePath, fileInfo);
    }

    /**
     * Получает информацию о файле из Redis.
     *
     * @param filePath путь к файлу
     * @return информация о файле
     */
    @Override
    public ExtractedFileInfo getFileInfo(Path filePath) {
        String key = buildFileInfoKey(filePath);
        try {
            String json = redisTemplate.opsForValue().get(key);
            if (json == null) {
                return null;
            }
            return objectMapper.readValue(json, ExtractedFileInfo.class);
        } catch (Exception e) {
            log.error("Ошибка при получении информации о файле {}: {}",
                    filePath, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Получает список ожидающих отправки файлов из Redis или пустой список.
     *
     * @return список ожидающих отправки файлов
     */
    @Override
    public List<ExtractedFileInfo> getPendingFiles() {
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
     * @param filePath путь к файлу
     * @param fileInfo информация о файле
     * @return true, если сохранено, false - если нет
     */
    private boolean saveFileInfo(Path filePath, ExtractedFileInfo fileInfo) {
        String key = buildFileInfoKey(filePath);
        try {
            String json = objectMapper.writeValueAsString(fileInfo);
            redisTemplate.opsForValue().set(key, json, TTL);
            return true;

        } catch (JsonProcessingException e) {
            log.error("Ошибка сериализации информации о файле {}: {}",
                    filePath, e.getMessage(), e);
            return false;

        } catch (Exception e) {
            log.error("Ошибка при сохранении информации о файле {}: {}",
                    filePath, e.getMessage(), e);
            return false;
        }
    }

    private String buildFileInfoKey(Path filePath) {
        return FILE_INFO_PREFIX + filePath.toAbsolutePath();
    }
}
