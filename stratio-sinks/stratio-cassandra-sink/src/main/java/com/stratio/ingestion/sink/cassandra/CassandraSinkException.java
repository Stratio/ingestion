package com.stratio.ingestion.sink.cassandra;

class CassandraSinkException extends RuntimeException {

    private static final long serialVersionUID = -5451808239155117379L;

    public CassandraSinkException(String message) {
        super(message);
    }

    public CassandraSinkException(Throwable cause) {
        super(cause);
    }

    public CassandraSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
