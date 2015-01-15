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
package com.stratio.ingestion.sink.mongodb;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

class DateFieldDefinition extends FieldDefinition {

    private static final long serialVersionUID = -5176021474260989590L;

    private DateFormat dateFormat;

    public DateFieldDefinition() {
        super(MongoDataType.DATE);
    }

    public DateFieldDefinition(String fieldName) {
        super(fieldName, MongoDataType.DATE);
    }

    public DateFormat getDateFormat() {
        return this.dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        setDateFormat(new SimpleDateFormat(dateFormat));
    }

    public void setDateFormat(DateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.fieldName == null ? 0 : this.fieldName.hashCode());
        result = prime * result + (this.mappedName == null ? 0 : this.mappedName.hashCode());
        result = prime * result + (this.dateFormat == null ? 0 : this.dateFormat.hashCode());
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
        DateFieldDefinition other = (DateFieldDefinition) obj;
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
