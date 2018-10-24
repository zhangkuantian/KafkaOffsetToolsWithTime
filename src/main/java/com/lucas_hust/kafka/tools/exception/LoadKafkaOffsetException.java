package com.lucas_hust.kafka.tools.exception;

public class LoadKafkaOffsetException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public LoadKafkaOffsetException(String message) {
        super(message);
    }

    public LoadKafkaOffsetException(String message, Throwable e) {
        super(message, e);
    }

    public LoadKafkaOffsetException(Throwable e) {
        super(e);
    }
}
