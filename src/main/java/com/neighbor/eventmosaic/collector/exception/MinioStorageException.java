package com.neighbor.eventmosaic.collector.exception;

/**
 * Исключение, сигнализирующее об ошибке во время операции с хранилищем MinIO.
 */
public class MinioStorageException extends EmCollectorException {

    public MinioStorageException(String message) {
        super(message);
    }

    public MinioStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
