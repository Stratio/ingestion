package com.stratio.ingestion.sink.cassandra;

class CassandraField<T> {

    private final String columnName;
    private final T value;

    public CassandraField(String columnName, T value) {
        this.columnName = columnName;
        this.value = value;
    }

    public T getValue() {
        return this.value;
    }

    public String getColumnName() {
        return this.columnName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.columnName == null ? 0 : this.columnName.hashCode());
        result = prime * result + (this.value == null ? 0 : this.value.hashCode());
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        CassandraField<T> other = (CassandraField<T>) obj;
        if (this.columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        } else if (!this.columnName.equals(other.columnName)) {
            return false;
        }
        if (this.value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!this.value.equals(other.value)) {
            return false;
        }
        return true;
    }
}
