package com.lucas_hust.kafka.tools.exception;

/**
 * Custom runtime exception for errors encountered during Kafka offset loading or CLI parsing.
 */
public class LoadKafkaOffsetException extends RuntimeException {

    private static final long serialVersionUID = 1L; // Standard practice for Serializable classes.

    /**
     * Constructs a new LoadKafkaOffsetException with the specified detail message.
     *
     * @param message The detail message.
     */
    public LoadKafkaOffsetException(String message) {
        super(message);
    }

    /**
     * Constructs a new LoadKafkaOffsetException with the specified detail message and cause.
     *
     * @param message The detail message.
     * @param cause   The cause of the exception.
     */
    public LoadKafkaOffsetException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new LoadKafkaOffsetException with the specified cause.
     *
     * @param cause The cause of the exception.
     */
    public LoadKafkaOffsetException(Throwable cause) {
        super(cause);
    }

}
