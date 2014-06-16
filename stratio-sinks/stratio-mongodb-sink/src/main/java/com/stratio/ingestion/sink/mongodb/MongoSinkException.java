package com.stratio.ingestion.sink.mongodb;

/**
 * This exception is raised whenever there is a problem with
 * {@link com.stratio.ingestion.sink.mongodb.MongoSink}.
 */
public class MongoSinkException extends RuntimeException {

    public MongoSinkException(String message) {
        super(message);
    }

    public MongoSinkException(Throwable cause) {
        super(cause);
    }

    public MongoSinkException(String message, Throwable cause) {
        super(message, cause);
    }

}
