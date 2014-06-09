package com.stratio.ingestion.sink.cassandra;

import java.util.List;

class CassandraRow {

    @SuppressWarnings("rawtypes")
    private final List<CassandraField> fields;

    @SuppressWarnings("rawtypes")
    public CassandraRow(List<CassandraField> fields) {
        this.fields = fields;
    }

    @SuppressWarnings("rawtypes")
    public List<CassandraField> getFields() {
        return this.fields;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.fields == null ? 0 : this.fields.hashCode());
        return result;
    }

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
        CassandraRow other = (CassandraRow) obj;
        if (this.fields == null) {
            if (other.fields != null) {
                return false;
            }
        } else if (!this.fields.equals(other.fields)) {
            return false;
        }
        return true;
    }
}
