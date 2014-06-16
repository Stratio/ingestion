package com.stratio.ingestion.sink.mongodb;

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
