package com.neighbor.eventmosaic.collector.exception;

public class EmCollectorException extends RuntimeException {

    public EmCollectorException(String message) {
        super(message);
    }

    public EmCollectorException(String message, Throwable cause) {
        super(message, cause);
    }
}
