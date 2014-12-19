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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;

class MappingDefinition implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(MappingDefinition.class);

    private static final long serialVersionUID = 1L;

    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";
    private static final String MAPPED_FROM = "mappedFrom";
    private static final String ENCODING = "encoding";
    private static final String BODY_TYPE = "bodyType";
    private static final String BODY_ENCODING = "bodyEncoding";
    private static final String BODY_FIELD = "bodyField";
    private static final String DATE_FORMAT = "dateFormat";

    private static final boolean DEFAULT_ADDITIONAL_PROPERTIES = true;
    private static final MongoDataType DEFAULT_BODY_TYPE = MongoDataType.BINARY;
    private static final String DEFAULT_BODY_ENCODING = "UTF-8"; // For BINARY it defaults to "raw".
    private static final String DEFAULT_BODY_FIELD = "data";
    private static final String DOCUMENT_MAPPING = "documentMapping";
    private static final String DELIMITER_CHAR = "delimiter";

    private final boolean additionalProperties;
    private final List<FieldDefinition> fields;
    private final MongoDataType bodyType;
    private final String bodyEncoding;
    private final String bodyField;

    private static final JsonNode EMPTY_SCHEMA = new ObjectNode(JsonNodeFactory.instance);

    public MappingDefinition() {
        this(EMPTY_SCHEMA);
    }

    public MappingDefinition(JsonNode jsonSchema) {
        if (jsonSchema.has(ADDITIONAL_PROPERTIES)) {
            this.additionalProperties = jsonSchema.get(ADDITIONAL_PROPERTIES).asBoolean();
        } else {
            this.additionalProperties = DEFAULT_ADDITIONAL_PROPERTIES;
        }
        if (jsonSchema.has(BODY_TYPE)) {
            this.bodyType = MongoDataType.valueOf(jsonSchema.get(BODY_TYPE).asText().toUpperCase(Locale.ENGLISH));
        } else {
            this.bodyType = DEFAULT_BODY_TYPE;
        }
        if (jsonSchema.has(BODY_ENCODING)) {
            this.bodyEncoding = jsonSchema.get(BODY_ENCODING).asText();
        } else {
            if (this.bodyType == MongoDataType.BINARY) {
                this.bodyEncoding = "raw";
            } else {
                this.bodyEncoding = DEFAULT_BODY_ENCODING;
            }
        }
        if (!"raw".equals(bodyEncoding) && !Charset.isSupported(bodyEncoding)) {
            throw new MongoSinkException("Unsupported charset: " + bodyEncoding);
        }
        if (jsonSchema.has(BODY_FIELD)) {
            this.bodyField = jsonSchema.get(BODY_FIELD).asText();
        } else {
            this.bodyField = DEFAULT_BODY_FIELD;
        }
        List<FieldDefinition> fields = new ArrayList<FieldDefinition>();
        if (jsonSchema.has(PROPERTIES)) {
            for (Map.Entry<String, JsonNode> entry : ImmutableList.copyOf(jsonSchema.get(PROPERTIES).fields())) {
                FieldDefinition fieldDefinition = populateFieldDefinition(entry);
                fields.add(fieldDefinition);
            }
        }
        this.fields = ImmutableList.copyOf(fields);
    }

    private FieldDefinition populateFieldDefinition(Map.Entry<String, JsonNode> entry) {
        String type = entry.getValue().get(TYPE).asText().toUpperCase(Locale.ENGLISH);
        FieldDefinition fieldDefinition = new FieldDefinition(entry.getKey(), MongoDataType.valueOf(type));

        populateMappedFromField(entry, fieldDefinition);
        populateEncodingField(entry, fieldDefinition);
        populateDateFormatField(entry, fieldDefinition);
        populateDocumentType(entry, fieldDefinition);

        return fieldDefinition;
    }

    private void populateDocumentType(Map.Entry<String, JsonNode> entry, FieldDefinition fieldDefinition) {
        if (fieldDefinition.getType().equals(MongoDataType.DOCUMENT) && entry.getValue().has(DOCUMENT_MAPPING)) {
            if (entry.getValue().has(DELIMITER_CHAR)) {
                fieldDefinition.setDelimiter(entry.getValue().get(DELIMITER_CHAR).asText());
                JsonNode documentMapping = entry.getValue().get(DOCUMENT_MAPPING);
                Map<String, FieldDefinition> documentFieldDefinitionMap = new LinkedHashMap<String, FieldDefinition>();
                Iterator<Map.Entry<String, JsonNode>> entryIterator = documentMapping.fields();
                Map.Entry<String, JsonNode> field = null;
                while (entryIterator.hasNext()) {
                    field = entryIterator.next();
                    documentFieldDefinitionMap.put(field.getKey(), populateFieldDefinition(field));
                }
                fieldDefinition.setDocumentMapping(documentFieldDefinitionMap);
            } else {
                throw new MongoSinkException("Delimiter char must be set into schema");
            }
        }
    }

    private void populateDateFormatField(Map.Entry<String, JsonNode> entry, FieldDefinition fieldDefinition) {
        if (fieldDefinition.getType().equals(MongoDataType.DATE) && entry.getValue().has(DATE_FORMAT)) {
            fieldDefinition.setDateFormat(entry.getValue().get(DATE_FORMAT).asText());
        }
    }

    private void populateEncodingField(Map.Entry<String, JsonNode> entry, FieldDefinition fieldDefinition) {
        if (fieldDefinition.getType().equals(MongoDataType.BINARY) && entry.getValue().has(ENCODING)) {
            fieldDefinition.setEncoding(entry.getValue().get(ENCODING).asText());
        }
    }

    private void populateMappedFromField(Map.Entry<String, JsonNode> entry, FieldDefinition fieldDefinition) {
        if (entry.getValue().has(MAPPED_FROM)) {
            fieldDefinition.setFieldName(entry.getValue().get(MAPPED_FROM).asText());
            fieldDefinition.setMappedName(entry.getKey());
        }
    }

    public List<FieldDefinition> getFields() {
        return this.fields;
    }

    public FieldDefinition getFieldDefinitionByName(String name) {
        for (FieldDefinition fd : this.fields) {
            if (fd.getFieldName().equals(name)) {
                return fd;
            }
        }
        return null;
    }

    public boolean allowsAdditionalProperties() {
        return this.additionalProperties;
    }

    public MongoDataType getBodyType() {
        return this.bodyType;
    }

    public String getBodyEncoding() {
        return this.bodyEncoding;
    }

    public String getBodyField() {
        return this.bodyField;
    }

    public static MappingDefinition load(final String path) {
        InputStream mappingInputstream = null;
        try {
            File mappingFile = new File(path);
            if (mappingFile.exists()) {
                mappingInputstream = new FileInputStream(mappingFile);
                log.debug("Loaded mapping definition from file: {}", path);
            } else {
                mappingInputstream = MappingDefinition.class.getResourceAsStream(path);
                if (mappingInputstream == null) {
                    throw new MongoSinkException("Cannot find file or resource for mapping definition: " + path);
                }
                log.debug("Loaded mapping definition from resource: {}", path);
            }
            return new MappingDefinition(new ObjectMapper().readTree(mappingInputstream));
        } catch (IOException ex) {
            throw new MongoSinkException(ex);
        } catch (IllegalArgumentException ex) {
            throw new MongoSinkException(ex);
        } finally {
            try {
                Closeables.close(mappingInputstream, true);
            } catch (IOException ex) {
                // Ignore
            }
        }
    }

}
