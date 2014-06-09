package com.stratio.ingestion.sink.cassandra;

enum CassandraDataType {

    TEXT("TEXT"),
    VARCHAR("VARCHAR"),
    VARINT("VARINT"),
    ASCII("ASCII"),
    BOOLEAN("BOOLEAN"),
    DECIMAL("DECIMAL"),
    DOUBLE("DOUBLE"),
    FLOAT("FLOAT"),
    INET("INET"),
    INT("INT"),
    COUNTER("COUNTER"),
    LIST("LIST"),
    MAP("MAP"),
    SET("SET"),
    TIMESTAMP("TIMESTAMP"),
    UUID("UUID"),
    BIGINT("BIGINT");

    private String type;

    private CassandraDataType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return this.type;
    }
}
