package com.stratio.ingestion.sink.jdbc;

/**
 * This exception is raised whenever there is a problem with
 * {@link com.stratio.ingestion.sink.jdbc.JDBCSink}.
 */
public class JDBCSinkException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JDBCSinkException(String message) {
        super(message);
    }

    public JDBCSinkException(Throwable cause) {
        super(cause);
    }

    public JDBCSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
