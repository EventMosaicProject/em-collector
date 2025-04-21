package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.service.HashStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Реализация сервиса хранения хешей на базе Redis.
 * Использует Redis для хранения хешей архивов GDELT.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisHashStoreServiceImpl implements HashStoreService {

    private static final String KEY_PREFIX = "gdelt:archive:hash:";
    private static final Duration TTL = Duration.ofDays(7);

    private final StringRedisTemplate redisTemplate;

    /**
     * Получает хеш архива из Redis.
     *
     * @param archiveName имя архива
     * @return хеш архива
     */
    @Override
    public String getStoredHash(String archiveName) {
        String key = buildKey(archiveName);
        String hash = redisTemplate.opsForValue().get(key);
        log.debug("Получен хеш {} для архива {}", hash, archiveName);
        return hash;
    }

    /**
     * Сохраняет хеш архива в Redis.
     *
     * @param archiveName имя архива
     * @param hash        хеш архива
     */
    @Override
    public void storeHash(String archiveName, String hash) {
        String key = buildKey(archiveName);
        redisTemplate.opsForValue().set(key, hash, TTL);
        log.debug("Сохранен хеш {} для архива {} с TTL {}", hash, archiveName, TTL);
    }

    /**
     * Проверяет, новый или изменен ли архив.
     *
     * @param archiveName имя архива
     * @param currentHash текущий хеш
     * @return true, если новый или изменен, false - если нет
     */
    @Override
    public boolean isNewOrChanged(String archiveName, String currentHash) {
        String storedHash = getStoredHash(archiveName);
        boolean isNewOrChanged = storedHash == null || !storedHash.equals(currentHash);
        log.debug("Архив {} новый или изменен: {} (сохраненный хеш: {}, текущий хеш: {})",
                archiveName, isNewOrChanged, storedHash, currentHash);
        return isNewOrChanged;
    }

    /**
     * Формирует ключ Redis для хеша архива.
     *
     * @param archiveName Имя архива
     * @return Ключ Redis
     */
    private String buildKey(String archiveName) {
        return KEY_PREFIX + archiveName;
    }
}