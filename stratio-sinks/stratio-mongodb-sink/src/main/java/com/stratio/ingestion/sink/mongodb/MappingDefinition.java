package com.stratio.ingestion.sink.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
        List<FieldDefinition> fields = new ArrayList<>();
        if (jsonSchema.has(PROPERTIES)) {
            for (Map.Entry<String, JsonNode> entry : ImmutableList.copyOf(jsonSchema.get(PROPERTIES).fields())) {
                FieldDefinition fieldDefinition = new FieldDefinition();
                fieldDefinition.setFieldName(entry.getKey());

                String type = entry.getValue().get(TYPE).asText().toUpperCase(Locale.ENGLISH);
                fieldDefinition.setType(MongoDataType.valueOf(type));

                if (entry.getValue().has(MAPPED_FROM)) {
                    fieldDefinition.setFieldName(entry.getValue().get(MAPPED_FROM).asText());
                    fieldDefinition.setMappedName(entry.getKey());
                }

                if (fieldDefinition.getType().equals(MongoDataType.BINARY) && entry.getValue().has(ENCODING)) {
                    fieldDefinition.setEncoding(entry.getValue().get(ENCODING).asText());
                }

                if (fieldDefinition.getType().equals(MongoDataType.DATE) && entry.getValue().has(DATE_FORMAT)) {
                    fieldDefinition.setDateFormat(entry.getValue().get(DATE_FORMAT).asText());
                }

                fields.add(fieldDefinition);
            }
        }
        this.fields = ImmutableList.copyOf(fields);
    }

	public List<FieldDefinition> getFields() {
        return this.fields;
	}

    public FieldDefinition getFieldDefinitionByName(String name) {
        for (FieldDefinition fd : this.fields)
            if (fd.getFieldName().equals(name))
                return fd;
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
        try {
            InputStream mappingInputstream;
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
        } catch (IOException | IllegalArgumentException ex) {
            throw new MongoSinkException(ex);
        }
    }

}