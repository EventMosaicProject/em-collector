package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.exception.GdeltProcessingException;
import com.neighbor.eventmosaic.collector.service.FileSystemService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Реализация сервиса работы с файловой системой.
 * Обеспечивает операции загрузки, расчета хешей и распаковки архивов.
 */
@Slf4j
@Service
public class FileSystemServiceImpl implements FileSystemService {

    /**
     * Константы для работы с файлами и хешированием
     */
    private static final int BUFFER_SIZE = 8192;
    private static final String HASH_ALGORITHM = "MD5";
    private static final Duration HTTP_TIMEOUT = Duration.ofMinutes(2);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(HTTP_TIMEOUT)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    /**
     * Загружает файл по-указанному URL с использованием HttpClient.
     * Реализует потоковую загрузку с буферизацией для работы с файлами большого размера.
     *
     * @param url        URL-адрес файла для загрузки
     * @param targetPath путь для сохранения файла
     * @return путь к загруженному файлу
     * @throws IOException при ошибках ввода-вывода или неудачном скачивании
     */
    @Override
    public Path downloadFile(String url, String targetPath) throws IOException {
        log.info("Начало загрузки файла с URL: {} в {}", url, targetPath);
        Path target = Paths.get(targetPath);

        try {
            // Создаем директорию для файла, если она не существует
            Files.createDirectories(target.getParent());

            // Создаем HTTP-запрос
            HttpRequest request = createHttpRequest(url);

            // Выполняем запрос и обрабатываем ответ
            downloadWithBuffering(httpClient, request, target);

            log.info("Файл успешно загружен: {}", targetPath);
            return target;

        } catch (IOException e) {
            log.error("Ошибка ввода-вывода при загрузке файла {}: {}", url, e.getMessage(), e);
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Загрузка файла {} прервана: {}", url, e.getMessage(), e);
            throw new IOException("Загрузка файла прервана", e);
        } catch (Exception e) {
            log.error("Непредвиденная ошибка при загрузке файла {}: {}", url, e.getMessage(), e);
            throw new IOException("Ошибка при загрузке файла: " + e.getMessage(), e);
        }
    }

    /**
     * Вычисляет MD5-хеш для указанного файла с использованием буферизованного чтения.
     * Метод оптимизирован для работы с большими файлами и потребляет
     * минимальное количество памяти независимо от размера файла.
     *
     * @param filePath путь к файлу
     * @return MD5-хеш файла в виде шестнадцатеричной строки
     * @throws IOException при ошибках ввода-вывода
     */
    @Override
    public String calculateMd5(Path filePath) throws IOException {
        log.debug("Расчет MD5-хеша для файла: {}", filePath);
        try {
            MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;

            try (InputStream is = Files.newInputStream(filePath)) {
                while ((bytesRead = is.read(buffer)) != -1) {
                    md.update(buffer, 0, bytesRead);
                }
            }

            byte[] digest = md.digest();
            String hash = bytesToHex(digest);
            log.debug("MD5-хеш файла {}: {}", filePath, hash);

            return hash;

        } catch (NoSuchAlgorithmException e) {
            // Теоретически невозможно, так как MD5 - стандартный алгоритм
            log.error("Алгоритм хеширования {} не найден", HASH_ALGORITHM, e);
            throw new GdeltProcessingException("Ошибка при получении алгоритма хеширования", e);
        } catch (IOException e) {
            log.error("Ошибка при чтении файла для расчета хеша: {}", filePath, e);
            throw e;
        }
    }

    /**
     * Распаковывает ZIP-архив в указанную директорию.
     * Метод использует буферизованное чтение для эффективной работы
     * с архивами большого размера, минимизируя потребление памяти.
     * Также включает проверку безопасности для предотвращения уязвимости "Zip Slip".
     *
     * @param zipFile   путь к ZIP-архиву
     * @param targetDir директория для распаковки
     * @return список путей к распакованным файлам
     * @throws IOException при ошибках ввода-вывода или некорректном формате архива,
     *                     или если архив содержит недопустимые пути (Zip Slip)
     */
    @Override
    public List<Path> extractZipFile(Path zipFile, Path targetDir) throws IOException {
        log.info("Распаковка архива {} в директорию {}", zipFile, targetDir);
        List<Path> extractedFiles = new ArrayList<>();

        // Преобразуем target директорию в абсолютный путь для надежной проверки
        Path absoluteTargetDir = targetDir
                .toAbsolutePath()
                .normalize();

        try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                /*
                ZIP-архив может содержать файл с путём вроде ../../dir/dir,
                и без нормализации он может записаться за пределами targetDir.
                Добавление normalize() и проверки startsWith предотвращает подобную атаку.
                 */
                Path resolvedPath = absoluteTargetDir
                        .resolve(entry.getName())
                        .normalize();

                // Проверка безопасности Zip Slip: убеждаемся, что путь находится ВНУТРИ целевой директории, например,
                // /targetDir/dir/file.txt, а не /targetDir/../../file.txt
                if (!resolvedPath.startsWith(absoluteTargetDir)) {
                    throw new IOException("Обнаружена попытка Zip Slip! Недопустимый путь внутри архива: " + entry.getName());
                }

                if (entry.isDirectory()) {
                    Files.createDirectories(resolvedPath);
                } else {
                    // Создаем родительские директории, если они еще не существуют
                    Files.createDirectories(resolvedPath.getParent());
                    extractFileFromZip(zipIn, resolvedPath);
                    extractedFiles.add(resolvedPath);
                    log.debug("Извлечен файл: {}", resolvedPath);
                }
                zipIn.closeEntry();
            }
        } catch (IOException e) {
            log.error("Ошибка при распаковке архива {}: {}", zipFile, e.getMessage(), e);
            throw e;
        }

        log.info("Архив успешно распакован. Извлечено файлов: {}", extractedFiles.size());
        return extractedFiles;
    }

    /**
     * Создает директорию, если она не существует.
     * Метод рекурсивно создает все несуществующие родительские директории.
     *
     * @param directory путь к директории
     * @return путь к созданной или существующей директории
     * @throws IOException при ошибках ввода-вывода
     */
    @Override
    public Path checkAndCreateDirectory(String directory) throws IOException {
        Path dir = Paths.get(directory);
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            log.info("Создание директории: {}", directory);
            Files.createDirectories(dir);
        }
        return dir;
    }

    /**
     * Преобразует массив байтов в шестнадцатеричную строку.
     *
     * @param bytes массив байтов для преобразования
     * @return шестнадцатеричная строка
     */
    private String bytesToHex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    /**
     * Создает HTTP-запрос для получения файла.
     *
     * @param url URL-адрес файла
     * @return HTTP-запрос
     */
    private HttpRequest createHttpRequest(String url) {
        return HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url))
                .timeout(HTTP_TIMEOUT)
                .build();
    }

    /**
     * Выполняет загрузку файла с буферизацией.
     * Метод потоково читает данные из HTTP-ответа и записывает их в файл
     * с использованием буфера, что позволяет обрабатывать файлы большого размера.
     *
     * @param client  HTTP-клиент
     * @param request HTTP-запрос
     * @param target  путь для сохранения файла
     * @throws IOException          при ошибках ввода-вывода
     * @throws InterruptedException если процесс загрузки был прерван
     */
    private void downloadWithBuffering(HttpClient client,
                                       HttpRequest request,
                                       Path target) throws IOException, InterruptedException {
        // Открываем потоковый ответ
        HttpResponse<InputStream> response = client.send(
                request,
                HttpResponse.BodyHandlers.ofInputStream());

        // Проверяем код ответа
        if (response.statusCode() != 200) {
            throw new IOException("Ошибка HTTP: код " + response.statusCode());
        }

        // Буферизированная запись в файл
        try (InputStream in = response.body();
             OutputStream out = Files.newOutputStream(target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }

            out.flush();
        }
    }

    /**
     * Извлекает файл из открытого ZIP-потока.
     * Метод использует буферизацию для эффективного извлечения,
     * что позволяет работать с файлами большого размера.
     *
     * @param zipIn    открытый ZIP-поток с текущей записью
     * @param filePath путь для сохранения извлекаемого файла
     * @throws IOException при ошибках ввода-вывода
     */
    private void extractFileFromZip(ZipInputStream zipIn, Path filePath) throws IOException {
        try (OutputStream out = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = zipIn.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
        }
    }
}