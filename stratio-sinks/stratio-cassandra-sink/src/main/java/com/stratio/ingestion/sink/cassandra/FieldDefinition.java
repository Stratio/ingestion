package com.stratio.ingestion.sink.cassandra;

import java.io.Serializable;

class FieldDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    private String type;
    private String columnName;
    private String dateFormat;
    private String itemSeparator;
    private String mapValueSeparator;
    private String mapKeyType;
    private String mapValueType;
    private String listValueType;

    public String getType() {
        return this.type;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getDateFormat() {
        return this.dateFormat;
    }

    public String getItemSeparator() {
        return this.itemSeparator;
    }

    public String getMapValueSeparator() {
        return this.mapValueSeparator;
    }

    public String getMapKeyType() {
        return this.mapKeyType;
    }

    public String getMapValueType() {
        return this.mapValueType;
    }

    public String getListValueType() {
        return this.listValueType;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public void setItemSeparator(String itemSeparator) {
        this.itemSeparator = itemSeparator;
    }

    public void setMapValueSeparator(String mapValueSeparator) {
        this.mapValueSeparator = mapValueSeparator;
    }

    public void setMapKeyType(String mapKeyType) {
        this.mapKeyType = mapKeyType;
    }

    public void setMapValueType(String mapValueType) {
        this.mapValueType = mapValueType;
    }

    public void setListValueType(String listValueType) {
        this.listValueType = listValueType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.columnName == null ? 0 : this.columnName.hashCode());
        result = prime * result + (this.dateFormat == null ? 0 : this.dateFormat.hashCode());
        result = prime * result + (this.itemSeparator == null ? 0 : this.itemSeparator.hashCode());
        result = prime * result + (this.listValueType == null ? 0 : this.listValueType.hashCode());
        result = prime * result + (this.mapKeyType == null ? 0 : this.mapKeyType.hashCode());
        result = prime * result
                + (this.mapValueSeparator == null ? 0 : this.mapValueSeparator.hashCode());
        result = prime * result + (this.mapValueType == null ? 0 : this.mapValueType.hashCode());
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
        if (this.columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        } else if (!this.columnName.equals(other.columnName)) {
            return false;
        }
        if (this.dateFormat == null) {
            if (other.dateFormat != null) {
                return false;
            }
        } else if (!this.dateFormat.equals(other.dateFormat)) {
            return false;
        }
        if (this.itemSeparator == null) {
            if (other.itemSeparator != null) {
                return false;
            }
        } else if (!this.itemSeparator.equals(other.itemSeparator)) {
            return false;
        }
        if (this.listValueType == null) {
            if (other.listValueType != null) {
                return false;
            }
        } else if (!this.listValueType.equals(other.listValueType)) {
            return false;
        }
        if (this.mapKeyType == null) {
            if (other.mapKeyType != null) {
                return false;
            }
        } else if (!this.mapKeyType.equals(other.mapKeyType)) {
            return false;
        }
        if (this.mapValueSeparator == null) {
            if (other.mapValueSeparator != null) {
                return false;
            }
        } else if (!this.mapValueSeparator.equals(other.mapValueSeparator)) {
            return false;
        }
        if (this.mapValueType == null) {
            if (other.mapValueType != null) {
                return false;
            }
        } else if (!this.mapValueType.equals(other.mapValueType)) {
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
