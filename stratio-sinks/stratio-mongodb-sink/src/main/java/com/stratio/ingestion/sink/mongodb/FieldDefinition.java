package com.stratio.ingestion.sink.mongodb;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

class FieldDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    private MongoDataType type;
    private String fieldName;
    private String mappedName;
    private DateFormat dateFormat;
    private String encoding;

    public MongoDataType getType() {
        return this.type;
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public String getMappedName() {
        return this.mappedName;
    }

    public DateFormat getDateFormat() {
        return this.dateFormat;
    }

    public String getEncoding() { return this.encoding; }

    public void setType(MongoDataType type) {
        this.type = type;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setMappedName(String mappedName) {
        this.mappedName = mappedName;
    }

    public void setDateFormat(String dateFormat) {
        setDateFormat(new SimpleDateFormat(dateFormat));
    }

    public void setDateFormat(DateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }

    public void setEncoding(String encoding) { this.encoding = encoding; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.fieldName == null ? 0 : this.fieldName.hashCode());
        result = prime * result + (this.mappedName == null ? 0 : this.mappedName.hashCode());
        result = prime * result + (this.dateFormat == null ? 0 : this.dateFormat.hashCode());
        result = prime * result + (this.encoding == null ? 0 : this.encoding.hashCode());
        result = prime * result + (this.type == null ? 0 : this.type.hashCode());
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
        FieldDefinition other = (FieldDefinition) obj;
        if (this.fieldName == null) {
            if (other.fieldName != null) {
                return false;
            }
        } else if (!this.fieldName.equals(other.fieldName)) {
            return false;
        }
        if (this.mappedName == null) {
            if (other.mappedName != null) {
                return false;
            }
        } else if (!this.mappedName.equals(other.mappedName)) {
            return false;
        }
        if (this.dateFormat == null) {
            if (other.dateFormat != null) {
                return false;
            }
        } else if (!this.dateFormat.equals(other.dateFormat)) {
            return false;
        }
        if (this.encoding == null) {
            if (other.encoding != null) {
                return false;
            }
        } else if (!this.encoding.equals(other.encoding)) {
            return false;
        }
        if (this.type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!this.type.equals(other.type)) {
            return false;
        }
        return true;
    }
}
