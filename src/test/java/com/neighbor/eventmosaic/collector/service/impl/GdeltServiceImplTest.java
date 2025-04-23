package com.neighbor.eventmosaic.collector.service.impl;

import com.neighbor.eventmosaic.collector.client.GdeltClient;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveInfo;
import com.neighbor.eventmosaic.collector.dto.GdeltArchiveProcessResult;
import com.neighbor.eventmosaic.collector.service.ArchiveService;
import com.neighbor.eventmosaic.collector.service.HashStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit-тесты для {@link GdeltServiceImpl}.
 */
@DisplayName("Unit-тесты для GdeltServiceImpl")
@ExtendWith(MockitoExtension.class)
class GdeltServiceImplTest {

    @Mock
    private GdeltClient gdeltClient;

    @Mock
    private HashStoreService hashStoreService;

    @Mock
    private ArchiveService archiveService;

    private GdeltServiceImpl gdeltService;

    @BeforeEach
    void setUp() {
        gdeltService = new GdeltServiceImpl(gdeltClient, hashStoreService, archiveService);
    }

    @Test
    @DisplayName("Должен ничего не делать, если список архивов пуст")
    void processLatestArchives_shouldDoNothing_ifArchiveListIsEmpty() throws Exception {
        // Arrange
        when(gdeltClient.getLatestArchivesList()).thenReturn(""); // пустой ответ

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get(); // ждем завершения

        // Assert
        verifyNoInteractions(hashStoreService, archiveService);
    }

    @Test
    @DisplayName("Должен игнорировать некорректные строки из GdeltClient")
    void processLatestArchives_shouldIgnoreInvalidLinesFromGdeltClient() throws Exception {
        // Arrange: одна строка валидная, две невалидные
        String content = """
                некорректная строка
                12345 hash1 http://data.gdeltproject.org/gdeltv2/20250323151500.translation.export.CSV.zip
                12345
                """;
        when(gdeltClient.getLatestArchivesList())
                .thenReturn(content);
        when(hashStoreService.isNewOrChanged(anyString(), anyString()))
                .thenReturn(true);
        when(archiveService.processArchiveAsync(any()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                GdeltArchiveProcessResult.success(mock(GdeltArchiveInfo.class), List.of())
                        )
                );

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get();

        // Assert: должен быть вызван только для одной валидной строки
        verify(archiveService, times(1)).processArchiveAsync(any());
    }

    @Test
    @DisplayName("Должен фильтровать архивы по поддерживаемому типу")
    void processLatestArchives_shouldFilterArchivesBySupportedType() throws Exception {
        // Arrange: одна поддерживаемая, одна неподдерживаемая
        String content = """
                12345 hash1 http://data.gdeltproject.org/gdeltv2/20250323151500.translation.export.CSV.zip
                12345 hash2 http://data.gdeltproject.org/gdeltv2/20250323151500.unsupportedtype.zip
                """;
        when(gdeltClient.getLatestArchivesList())
                .thenReturn(content);
        when(hashStoreService.isNewOrChanged(anyString(), anyString()))
                .thenReturn(true);
        when(archiveService.processArchiveAsync(any()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                GdeltArchiveProcessResult.success(mock(GdeltArchiveInfo.class), List.of())
                        )
                );

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get();

        // Assert: только поддерживаемый тип
        verify(archiveService, times(1)).processArchiveAsync(argThat(info ->
                info.url().contains("translation.export.CSV.zip")));
    }

    @Test
    @DisplayName("Должен отбирать только новые или изменённые архивы")
    void processLatestArchives_shouldSelectOnlyNewOrChangedArchives() throws Exception {
        // Arrange: два архива, только один новый/изменённый
        String content = """
                12345 hash1 http://data.gdeltproject.org/gdeltv2/20250323151500.translation.export.CSV.zip
                12345 hash2 http://data.gdeltproject.org/gdeltv2/20250323153000.translation.export.CSV.zip
                """;
        when(gdeltClient.getLatestArchivesList())
                .thenReturn(content);
        when(hashStoreService.isNewOrChanged(anyString(), eq("hash1")))
                .thenReturn(false);
        when(hashStoreService.isNewOrChanged(anyString(), eq("hash2")))
                .thenReturn(true);
        when(archiveService.processArchiveAsync(any()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                GdeltArchiveProcessResult.success(mock(GdeltArchiveInfo.class), List.of())
                        )
                );

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get();

        // Assert: только для второго архива
        verify(archiveService, times(1)).processArchiveAsync(argThat(info ->
                info.hash().equals("hash2")));
    }

    @Test
    @DisplayName("Должен корректно обрабатывать ошибку в одном архиве и продолжать обработку остальных")
    void processLatestArchives_shouldHandleErrorInOneArchiveAndContinueProcessingOthers() throws Exception {
        // Arrange: два архива, один падает
        String content = """
                12345 hash1 http://data.gdeltproject.org/gdeltv2/20250323151500.translation.export.CSV.zip
                12345 hash2 http://data.gdeltproject.org/gdeltv2/20250323153000.translation.export.CSV.zip
                """;
        when(gdeltClient.getLatestArchivesList())
                .thenReturn(content);
        when(hashStoreService.isNewOrChanged(anyString(), anyString()))
                .thenReturn(true);
        when(archiveService.processArchiveAsync(withHash("hash1")))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                GdeltArchiveProcessResult.failure(mock(GdeltArchiveInfo.class), "Ошибка")
                        )
                );
        when(archiveService.processArchiveAsync(withHash("hash2")))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                GdeltArchiveProcessResult.success(mock(GdeltArchiveInfo.class), List.of())
                        )
                );

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get();

        // Assert: оба вызова были, несмотря на ошибку в одном
        verify(archiveService, times(2)).processArchiveAsync(any());
    }

    @Test
    @DisplayName("Должен вызывать processArchiveAsync для нужных архивов")
    void processLatestArchives_shouldCallProcessArchiveAsyncForSelectedArchives() throws Exception {
        // Arrange: три архива, два новых
        String content = """
                12345 hash1 http://data.gdeltproject.org/gdeltv2/20250323151500.translation.export.CSV.zip
                12345 hash2 http://data.gdeltproject.org/gdeltv2/20250323153000.translation.export.CSV.zip
                12345 hash3 http://data.gdeltproject.org/gdeltv2/20250323154500.translation.export.CSV.zip
                """;
        when(gdeltClient.getLatestArchivesList())
                .thenReturn(content);
        when(hashStoreService.isNewOrChanged(anyString(), eq("hash1")))
                .thenReturn(false);
        when(hashStoreService.isNewOrChanged(anyString(), eq("hash2")))
                .thenReturn(true);
        when(hashStoreService.isNewOrChanged(anyString(), eq("hash3")))
                .thenReturn(true);
        when(archiveService.processArchiveAsync(any()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                GdeltArchiveProcessResult.success(mock(GdeltArchiveInfo.class), List.of())
                        )
                );

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get();

        // Assert: только для новых/изменённых (hash2, hash3)
        ArgumentCaptor<GdeltArchiveInfo> captor = ArgumentCaptor.forClass(GdeltArchiveInfo.class);
        verify(archiveService, times(2)).processArchiveAsync(captor.capture());
        List<GdeltArchiveInfo> called = captor.getAllValues();
        assertThat(called).extracting(GdeltArchiveInfo::hash)
                .containsExactlyInAnyOrder("hash2", "hash3");
    }

    @Test
    @DisplayName("Должен корректно анализировать результаты обработки архивов")
    @SuppressWarnings("unchecked")
    void processLatestArchives_shouldAnalyzeProcessingResultsCorrectly() throws Exception {
        // Arrange: два архива, один успешный, один с ошибкой
        String content = """
                12345 hash1 http://data.gdeltproject.org/gdeltv2/20250323151500.translation.export.CSV.zip
                12345 hash2 http://data.gdeltproject.org/gdeltv2/20250323153000.translation.export.CSV.zip
                """;
        when(gdeltClient.getLatestArchivesList())
                .thenReturn(content);
        when(hashStoreService.isNewOrChanged(anyString(), anyString()))
                .thenReturn(true);
        when(archiveService.processArchiveAsync(any()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                GdeltArchiveProcessResult.success(mock(GdeltArchiveInfo.class), List.of())
                        ),
                        CompletableFuture.completedFuture(
                                GdeltArchiveProcessResult.failure(mock(GdeltArchiveInfo.class), "Ошибка")
                        )
                );

        // Act
        CompletableFuture<Void> future = gdeltService.processLatestArchives();
        future.get();

        // Assert: оба вызова были, результат успешный и неуспешный
        verify(archiveService, times(2)).processArchiveAsync(any());
    }

    @Test
    @DisplayName("Должен корректно обрабатывать исключения верхнего уровня")
    void processLatestArchives_shouldHandleTopLevelExceptionsProperly() {
        // Arrange: GdeltClient выбрасывает исключение
        when(gdeltClient.getLatestArchivesList())
                .thenThrow(new RuntimeException("Ошибка сети"));

        // Act & Assert
        assertThatThrownBy(() -> gdeltService.processLatestArchives().join())
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Ошибка сети");
    }

    private static GdeltArchiveInfo withHash(String expectedHash) {
        return argThat(info -> info != null && expectedHash.equals(info.hash()));
    }
} 