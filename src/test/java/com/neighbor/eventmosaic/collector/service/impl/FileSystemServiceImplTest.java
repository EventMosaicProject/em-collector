package com.neighbor.eventmosaic.collector.service.impl;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit-тесты для {@link FileSystemServiceImpl}.
 * <p>
 * Эти тесты проверяют функциональность сервиса работы с файловой системой,
 * включая создание директорий, вычисление MD5-хешей и извлечение ZIP-архивов.
 */
@DisplayName("Unit-тесты для FileSystemServiceImpl")
class FileSystemServiceImplTest {

    @TempDir
    Path tempDir;

    private FileSystemServiceImpl fileSystemService;
    private WireMockServer wireMockServer;

    @BeforeEach
    void setUp() {
        fileSystemService = new FileSystemServiceImpl();
    }

    @AfterEach
    void tearDown() {
        if (wireMockServer != null && wireMockServer.isRunning()) {
            wireMockServer.stop();
        }
    }

    @Test
    @DisplayName("checkAndCreateDirectory: создает директорию, если она не существует")
    void checkAndCreateDirectory_shouldCreateDirectoryIfNotExists() throws IOException {
        // Arrange
        Path newDir = tempDir.resolve("newDirectory");
        String newDirPath = newDir.toString();

        // Act
        Path result = fileSystemService.checkAndCreateDirectory(newDirPath);

        // Assert
        assertThat(Files.exists(result))
                .as("Директория должна быть создана")
                .isTrue();
        assertThat(Files.isDirectory(result))
                .as("Путь должен быть директорией")
                .isTrue();
        assertThat(result)
                .as("Возвращаемый путь должен совпадать с ожидаемым")
                .isEqualTo(newDir);
    }

    @Test
    @DisplayName("checkAndCreateDirectory: возвращает существующую директорию без ошибок")
    void checkAndCreateDirectory_shouldReturnExistingDirectory() throws IOException {
        // Arrange
        Path existingDir = tempDir.resolve("existingDirectory");
        Files.createDirectories(existingDir);
        String existingDirPath = existingDir.toString();

        // Act
        Path result = fileSystemService.checkAndCreateDirectory(existingDirPath);

        // Assert
        assertThat(result)
                .as("Возвращаемый путь должен совпадать с существующей директорией")
                .isEqualTo(existingDir);
        assertThat(Files.exists(result))
                .as("Директория должна существовать")
                .isTrue();
        assertThat(Files.isDirectory(result))
                .as("Путь должен быть директорией")
                .isTrue();
    }

    @Test
    @DisplayName("checkAndCreateDirectory: выбрасывает IOException если путь занят файлом")
    void checkAndCreateDirectory_shouldThrowIOExceptionIfPathIsFile() throws IOException {
        // Arrange
        Path filePath = tempDir.resolve("example.txt");
        Files.writeString(filePath, "test");

        // Act & Assert
        assertThatThrownBy(() -> fileSystemService.checkAndCreateDirectory(filePath.toString()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(filePath.getFileName().toString());
    }

    @Test
    @DisplayName("calculateMd5: корректно вычисляет MD5 для файла")
    void calculateMd5_shouldReturnCorrectHash() throws IOException {
        // Arrange
        String content = "Hello, GDELT!";
        Path file = tempDir.resolve("test.txt");
        Files.writeString(file, content, StandardOpenOption.CREATE_NEW);

        String expectedMd5 = "8e8ac5e9b5d7048e9de0d2e11c5581d8";

        // Act
        String actualMd5 = fileSystemService.calculateMd5(file);

        // Assert
        assertThat(actualMd5)
                .isEqualToIgnoringCase(expectedMd5);
    }

    @Test
    @DisplayName("calculateMd5: выбрасывает IOException для несуществующего файла")
    void calculateMd5_shouldThrowIOExceptionForNonExistentFile() {
        // Arrange
        Path nonExistentFile = tempDir.resolve("fake_file.txt");

        // Act & Assert
        assertThatThrownBy(() -> fileSystemService.calculateMd5(nonExistentFile))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("fake_file");
    }

    @Test
    @DisplayName("extractZipFile: корректно извлекает файлы из архива")
    void extractZipFile_shouldExtractFilesCorrectly() throws IOException {
        // Arrange
        Path zip = Path.of("src/test/resources/data/simple.zip");
        Path extractDir = tempDir.resolve("unzipped");

        // Act
        var extractedFiles = fileSystemService.extractZipFile(zip, extractDir);

        // Assert
        assertThat(extractedFiles)
                .isNotEmpty()
                .allSatisfy(path -> assertThat(Files.exists(path)).isTrue())
                .anyMatch(p -> p.getFileName().toString().equals("file1.txt"))
                .anyMatch(p -> p.getFileName().toString().equals("file2.txt"));

        // Проверяем содержимое одного из файлов
        Path file1 = extractDir.resolve("file1.txt");
        assertThat(Files.readString(file1)).isEqualTo("Hello from file1");
    }

    @Test
    @DisplayName("extractZipFile: выбрасывает IOException при попытке Zip Slip")
    void extractZipFile_shouldThrowIOExceptionOnZipSlip() {
        // Arrange
        Path zipSlip = Path.of("src/test/resources/data/zip-slip.zip");
        Path extractDir = tempDir.resolve("zip-slip-test");

        // Act & Assert
        assertThatThrownBy(() -> fileSystemService.extractZipFile(zipSlip, extractDir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Zip Slip");
    }

    @Test
    @DisplayName("extractZipFile: корректно обрабатывает пустой архив")
    void extractZipFile_shouldHandleEmptyZip() throws IOException {
        // Arrange
        Path emptyZip = Path.of("src/test/resources/data/empty.zip");
        Path extractDir = tempDir.resolve("empty-unzip");

        // Act
        List<Path> extractedFiles = fileSystemService.extractZipFile(emptyZip, extractDir);

        // Assert
        assertThat(extractedFiles)
                .isEmpty();
    }

    @Test
    @DisplayName("downloadFile: успешно скачивает файл по HTTP")
    void downloadFile_shouldDownloadFileSuccessfully() throws IOException {
        // Arrange
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();

        String fileContent = "test file content";
        String filePath = "/example.txt";

        wireMockServer.stubFor(
                WireMock.get(WireMock.urlEqualTo(filePath))
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)
                                .withBody(fileContent))
        );
        String url = "http://localhost:" + wireMockServer.port() + filePath;
        Path target = tempDir.resolve("downloaded.txt");

        // Act
        Path result = fileSystemService.downloadFile(url, target.toString());

        // Assert
        assertThat(Files.exists(result)).isTrue();
        assertThat(Files.readString(result)).isEqualTo(fileContent);
    }

    @Test
    @DisplayName("downloadFile: выбрасывает IOException при ошибке HTTP")
    void downloadFile_shouldThrowIOExceptionOnHttpError() {
        // Arrange
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();
        String filePath = "/notfound.txt";
        wireMockServer.stubFor(
                WireMock.get(WireMock.urlEqualTo(filePath))
                        .willReturn(WireMock.aResponse()
                                .withStatus(404))
        );
        String url = "http://localhost:" + wireMockServer.port() + filePath;
        Path target = tempDir.resolve("should_not_exist.txt");

        // Act & Assert
        assertThatThrownBy(() -> fileSystemService.downloadFile(url, target.toString()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("404");
    }
} 