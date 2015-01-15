/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.sink.cassandra;

import java.io.Serializable;

import com.datastax.driver.core.DataType;
import com.google.common.base.Strings;

class FieldDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    private String type;
    private String columnName;
    private String itemSeparator;
    private String mapValueSeparator;
    private String mapKeyType;
    private String mapValueType;
    private String listValueType;
    private String cassandraType;

    public String getType() {
        return this.type;
    }

    public String getColumnName() {
        return this.columnName;
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
    
    public String getCassandraType() {
    	if (Strings.isNullOrEmpty(this.cassandraType)) {
	    	String ctype = this.getType();
			switch (DataType.Name.valueOf(ctype)) {
			case LIST:
			case SET:
				ctype += "<" + this.getListValueType() + ">";
				break;
			case MAP:
				ctype += "<" + this.getMapKeyType() + "," + this.getMapValueType() + ">";
				break;
			default:
				break;
			}
			this.cassandraType = ctype;
    	}
		return this.cassandraType;
	}

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.columnName == null ? 0 : this.columnName.hashCode());
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
