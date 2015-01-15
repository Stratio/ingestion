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

import java.util.Map;

class DocumentFieldDefinition extends FieldDefinition {

    private static final long serialVersionUID = 5302068347108620110L;

    private Map<String, FieldDefinition> documentMapping;

    private String delimiter;

    public DocumentFieldDefinition() {
        super(MongoDataType.DOCUMENT);
    }

    public DocumentFieldDefinition(String fieldName) {
        super(fieldName, MongoDataType.DOCUMENT);
    }

    public Map<String, FieldDefinition> getDocumentMapping() {
        return documentMapping;
    }

    public void setDocumentMapping(Map<String, FieldDefinition> documentMapping) {
        this.documentMapping = documentMapping;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.fieldName == null ? 0 : this.fieldName.hashCode());
        result = prime * result + (this.mappedName == null ? 0 : this.mappedName.hashCode());
        result = prime * result + (this.documentMapping == null ? 0 : this.documentMapping.hashCode());
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
        DocumentFieldDefinition other = (DocumentFieldDefinition) obj;
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
        if (this.documentMapping == null) {
            if (other.documentMapping != null) {
                return false;
            }
        } else if (!this.documentMapping.equals(other.documentMapping)) {
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
