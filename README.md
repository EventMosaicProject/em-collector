# Сервис сбора данных (em-collector)

Микросервис `em-collector` отвечает за автоматическое получение, обработку и передачу информации о файлах данных из внешнего источника. Он регулярно проверяет наличие новых или обновленных архивов данных, загружает их, распаковывает, **сохраняет в S3-совместимое хранилище MinIO** и отправляет **URL-адреса** к этим файлам в соответствующие топики Apache Kafka для дальнейшей обработки другими сервисами.

## Основной рабочий процесс

1.  **Планирование и запуск:**
    *   Процесс инициируется либо автоматически по расписанию (`GdeltScheduler`, по умолчанию каждую минуту), либо вручную через REST API эндпоинт `POST /api/v1/gdelt/process` (`GdeltController`).
    *   Оба способа запускают основной метод `GdeltService.processLatestArchives()`.

2.  **Получение списка архивов:**
    *   Сервис (`GdeltService`) использует Feign-клиент (`GdeltClient`) для запроса текстового файла `lastupdate-translation.txt` с официального ресурса GDELT. Этот файл содержит информацию о последних доступных архивах: размер, MD5-хеш и URL для каждого архива.
    *   Feign-клиент настроен с механизмом повторных попыток (`FeignClientConfig`) для обработки временных сетевых проблем.

3.  **Парсинг и проверка необходимости загрузки:**
    *   Полученный текстовый ответ парсится в список объектов `GdeltArchiveInfo`, каждый из которых представляет один архив.
    *   Для каждого архива из списка сервис обращается к `HashStoreService` (реализация `RedisHashStoreServiceImpl`), чтобы проверить, является ли архив новым или его содержимое изменилось. Проверка осуществляется путем сравнения текущего MD5-хеша (из `lastupdate-translation.txt`) с хешем, сохраненным в Redis для этого имени архива (ключ вида `gdelt:archive:hash:{archiveName}`).

4.  **Загрузка и обработка архивов (асинхронно):**
    *   Архивы, которые определены как новые или измененные, обрабатываются асинхронно (с использованием `Executor` на базе виртуальных потоков, настроенного в `AppConfig`).
    *   Для каждого такого архива (`ArchiveServiceImpl`):
        *   **Создание директории для загрузки:** Сервис проверяет и при необходимости создает директорию для загрузки (`gdelt.storage.download-dir`).
        *   **Загрузка архива:** Архив загружается по URL с помощью `FileSystemService.downloadFile()`.
        *   **Проверка целостности:** После загрузки `FileSystemService.calculateMd5()` вычисляет MD5-хеш скачанного файла. Этот хеш сравнивается с ожидаемым. Если они не совпадают, процесс для этого архива завершается с ошибкой.
        *   **Распаковка во временную директорию:** Если проверка прошла успешно, `FileSystemService.extractZipFile()` распаковывает содержимое ZIP-архива во *временную локальную директорию*. Этот метод включает защиту от уязвимости "Zip Slip".
        *   **Загрузка в MinIO:** Каждый извлеченный файл загружается в S3-совместимое хранилище MinIO с помощью `MinioStorageService`. Сервис возвращает URL для каждого загруженного файла.
        *   **Публикация события:** При успешной загрузке всех файлов из архива в MinIO, публикуется событие `ArchiveExtractedEvent`, содержащее информацию об исходном архиве (`GdeltArchiveInfo`) и список **URL-адресов** к загруженным в MinIO файлам (`List<String>`).
        *   **Обновление хеша:** Новый MD5-хеш успешно обработанного архива сохраняется в Redis через `HashStoreService.storeHash()`.
        *   **Очистка:** Загруженный ZIP-архив и *временные локальные распакованные файлы* удаляются после успешной обработки.

5.  **Обработка события распаковки (асинхронно):**
    *   `ArchiveExtractedEventListener` асинхронно обрабатывает событие `ArchiveExtractedEvent`.
    *   Для каждого **URL извлеченного файла** (`String`) из события:
        *   **Определение топика Kafka:** `GdeltTopicResolver` определяет целевой топик Kafka на основе имени исходного архива.
        *   **Регистрация статуса отправки:** `FileSendStatusService` (реализация `RedisFileSendStatusServiceImpl`) регистрирует информацию о файле (используя его URL) в Redis (например, ключ `gdelt:file:url:{fileUrl}`). Создается запись `ExtractedFileInfo` со статусом `isSent = false`. Эта запись имеет TTL.
        *   **Отправка в Kafka:** Слушатель использует `KafkaMessageService` для отправки сообщения в определенный топик. Сообщение содержит **URL файла** в MinIO.
        *   **Обновление статуса:** При получении подтверждения от Kafka об успешной отправке, `FileSendStatusService.markAsSent()` (вызываемый из `KafkaMessageService`) обновляет статус файла в Redis на `isSent = true` (используя URL файла).

6.  **Механизм повторной отправки в Kafka:**
    *   `FileSendRetryScheduler` запускается по расписанию (`gdelt.retry.interval`).
    *   Он запрашивает у `FileSendStatusService` список всех файлов (по их URL), у которых статус `isSent = false`.
    *   Для каждого такого файла (представленного его URL) планировщик определяет нужный топик Kafka и использует `KafkaMessageService` для повторной отправки **URL файла** в Kafka.
    *   `KafkaMessageService` автоматически обновляет статус файла на `isSent = true` через `FileSendStatusService` в случае успешной отправки.

## Диаграмма последовательности (клик на кнопку ⟷ развернет схему)

```mermaid
sequenceDiagram
    participant Sched as GdeltScheduler
    participant Ctrl as GdeltController
    participant GServ as GdeltService
    participant ArchServ as ArchiveService
    participant Client as GdeltClient
    participant HashStore as HashStoreService
    participant FS as FileSystemService
    participant Minio as MinioStorageService
    participant EvtPub as EventPublisher
    participant EvtList as EventListener
    participant TopicRes as TopicResolver
    participant StatusStore as StatusService
    participant Kafka as Kafka
    participant RetrySched as RetryScheduler

    alt Запуск по расписанию
        Sched->>GServ: processLatestArchives()
    else Ручной запуск
        Ctrl->>GServ: processLatestArchives()
    end

    GServ->>Client: getLatestArchivesList()
    Client-->>GServ: archiveListText (список архивов в текстовом виде)
    GServ->>GServ: parse to GdeltArchiveInfo (парсинг в объекты)

    loop Для каждого архива в списке
        GServ->>HashStore: isNewOrChanged(имя_архива, хеш_архива)
        HashStore-->>GServ: needsProcessing (флаг, нужна ли обработка)
        
        alt Архив новый или изменен
            GServ->>ArchServ: processArchiveAsync(archiveInfo) # Делегирование обработки ArchServ
            
            ArchServ->>FS: checkAndCreateDirectory(downloadDir) # Проверка/создание папки загрузки
            ArchServ->>FS: downloadFile(url, targetPath) # Загрузка архива
            FS-->>ArchServ: downloadedArchivePath (путь к скачанному архиву)
            ArchServ->>FS: calculateMd5(downloadedArchivePath) # Расчет хеша скачанного архива
            FS-->>ArchServ: fileHash (рассчитанный хеш)
            
            alt Хеш совпадает
                ArchServ->>FS: extractZipFile(archivePath, tempExtractDir) # Распаковка во временную папку
                FS-->>ArchServ: localExtractedPaths (список путей к локальным распакованным файлам)
                
                loop Для каждого локально распакованного файла
                    ArchServ->>Minio: uploadFile(objectName, localPath) # Загрузка файла в MinIO
                    Minio-->>ArchServ: fileUrl (URL файла в MinIO)
                    ArchServ->>FS: deleteLocalFile(localPath) # Удаление временного локального файла
                end
                
                ArchServ->>EvtPub: publishEvent(ArchiveExtractedEvent with fileUrls) # Публикация события с URL файлов
                ArchServ->>HashStore: storeHash(archiveName, newHash) # Сохранение нового хеша в Redis
                ArchServ->>FS: deleteFile(downloadedArchivePath) # Удаление скачанного ZIP-архива
            else Хеш не совпадает
                ArchServ->>ArchServ: log error, return failure (логирование ошибки, возврат неудачи)
            end
            ArchServ-->>GServ: archiveProcessResult (результат обработки архива)
        end
    end

    EvtList->>EvtPub: listen event (подписка на событие)
    EvtPub-->>EvtList: ArchiveExtractedEvent (содержит fileUrls)
    
    loop Для каждого fileUrl из события
        EvtList->>TopicRes: resolveTopic(archiveFileName) # Определение топика Kafka
        TopicRes-->>EvtList: topicName (имя топика)
        EvtList->>StatusStore: registerFile(archiveFileName, fileUrl) # Регистрация файла для отслеживания
        EvtList->>Kafka: send(topicName, fileUrl) # Отправка URL файла в Kafka
        
        alt Успешная отправка (Kafka ACK)
            Kafka-->>EvtList: ack (подтверждение, через колбэк KafkaMessageService)
            EvtList->>StatusStore: markAsSent(fileUrl) # Обновление статуса на "отправлен" (неявно через KafkaMessageService)
        else Ошибка отправки
            Kafka-->>EvtList: error (ошибка, через колбэк KafkaMessageService)
            # StatusStore: статус файла остается "не отправлен"
        end
    end

    RetrySched->>StatusStore: getPendingFiles() # Запрос списка неотправленных файлов
    StatusStore-->>RetrySched: pendingFileInfos (информация о файлах, содержит fileUrls)
    
    loop Для каждого неотправленного файла (pendingFileInfo)
        RetrySched->>TopicRes: resolveTopic(archiveFileName) # Определение топика
        TopicRes-->>RetrySched: topicName (имя топика)
        RetrySched->>Kafka: send(topicName, fileUrl) # Повторная отправка URL в Kafka
            
        alt Успешная отправка (Kafka ACK)
            Kafka-->>RetrySched: ack (подтверждение, через колбэк KafkaMessageService)
            RetrySched->>StatusStore: markAsSent(fileUrl) # Обновление статуса (неявно через KafkaMessageService)
        else Ошибка отправки
            Kafka-->>RetrySched: error (ошибка, через колбэк KafkaMessageService)
            # StatusStore: статус файла остается "не отправлен"
        end
    end
```

