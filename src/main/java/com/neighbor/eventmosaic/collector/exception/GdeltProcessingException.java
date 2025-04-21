package com.neighbor.eventmosaic.collector.exception;

/**
 * Исключение, выбрасываемое при ошибках обработки данных GDELT.
 */
public class GdeltProcessingException extends RuntimeException {

    public GdeltProcessingException(String message) {
        super(message);
    }

    public GdeltProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}