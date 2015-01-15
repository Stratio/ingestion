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

import java.io.Serializable;

abstract class FieldDefinition implements Serializable {

    private static final long serialVersionUID = -7630057866092766830L;

    protected MongoDataType type;

    protected String fieldName;

    protected String mappedName;

    protected FieldDefinition() {
        super();
    }

    protected FieldDefinition(MongoDataType type) {
        this.type = type;
    }

    protected FieldDefinition(String fieldName, MongoDataType type) {
        this.type = type;
        this.fieldName = fieldName;
    }

    public MongoDataType getType() {
        return this.type;
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public String getMappedName() {
        return this.mappedName;
    }

    public void setType(MongoDataType type) {
        this.type = type;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setMappedName(String mappedName) {
        this.mappedName = mappedName;
    }
}
