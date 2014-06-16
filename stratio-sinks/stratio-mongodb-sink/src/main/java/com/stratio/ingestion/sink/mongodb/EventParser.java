package com.stratio.ingestion.sink.mongodb;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.apache.flume.Event;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;

class EventParser {

    private static final Logger log = LoggerFactory.getLogger(EventParser.class);

    private static final String DEFAULT_BINARY_ENCODING = "base64";

	private final MappingDefinition definition;

    public EventParser() {
        this(new MappingDefinition());
    }

	public EventParser(final MappingDefinition mappingDefinition) {
		this.definition = mappingDefinition;
	}

    public Object parseValue(final FieldDefinition fd, final String stringValue) {
        if (fd == null || fd.getType() == null) {
            try {
                return JSON.parse(stringValue);
            } catch (JSONParseException ex) {
                //XXX: Default to String
                log.trace("Could not parse as JSON, defaulting to String: {}", stringValue);
                return stringValue;
            }
        }
        switch (fd.getType()) {
            case DOUBLE:
                return Double.parseDouble(stringValue);
            case STRING:
                return stringValue;
            case OBJECT:
            case ARRAY:
                //TODO: should we use customizable array representation?
                //TODO: should we check that the result is indeed an array or object?
                return JSON.parse(stringValue);
            case BINARY:
                final String encoding = (fd.getEncoding() == null)? DEFAULT_BINARY_ENCODING : fd.getEncoding().toLowerCase(Locale.ENGLISH);
                if ("base64".equals(encoding)) {
                    return BaseEncoding.base64().decode(stringValue);
                } else {
                    throw new UnsupportedOperationException("Unsupported encoding for binary type: " + encoding);
                }
            //TODO: case "UNDEFINED":
            case OBJECTID:
                return new ObjectId(stringValue);
            case BOOLEAN:
                return Boolean.parseBoolean(stringValue);
            case DATE:
                DateFormat dateFormat = fd.getDateFormat();
                if (dateFormat == null) {
                    return new Date(Long.parseLong(stringValue));
                } else {
                    try {
                        return dateFormat.parse(stringValue);
                    } catch (ParseException ex) {
                        //XXX: Default to string
                        log.warn("Could not parse date, defaulting to String: {}", stringValue);
                        return stringValue;
                    }
                }
            case NULL:
                //TODO: Check if this is valid
                return null;
            //TODO: case "REGEX":
            //TODO: case "JAVASCRIPT":
            //TODO: case "SYMBOL":
            //TODO: case "JAVASCRIPT_SCOPE":
            case INT32:
                return Integer.parseInt(stringValue);
            case INT64:
                return Long.parseLong(stringValue);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fd.getType().name());
        }
    }

	public DBObject parse(Event event) {

        DBObject dbObject = new BasicDBObject();
        if (definition.getBodyType() != MongoDataType.NULL) {
            Object obj = null;
            if (definition.getBodyType() == MongoDataType.BINARY && definition.getBodyEncoding().equals("raw")) {
                obj = event.getBody();
            } else if (definition.getBodyType() == MongoDataType.STRING) {
                Charset charset = Charset.forName(definition.getBodyEncoding());
                obj = new String(event.getBody(), charset);
            } else {
                FieldDefinition fd = new FieldDefinition();
                fd.setType(definition.getBodyType());
                fd.setEncoding(definition.getBodyEncoding());
                obj = parseValue(fd, new String(event.getBody(), Charsets.UTF_8));
            }

            if (!"".equals(definition.getBodyField())) {
                dbObject.put(definition.getBodyField(), obj);
            } else if (obj instanceof DBObject) {
                dbObject = (DBObject) obj;
            } else {
                log.warn("Could not map body to JSON document: {}", obj);
            }
        }

		final Map<String, String> eventHeaders = event.getHeaders();
        if (definition.allowsAdditionalProperties()) {
            for (final String fieldName : eventHeaders.keySet()) {
                FieldDefinition def = definition.getFieldDefinitionByName(fieldName);
                if (def == null) {
                    dbObject.put(fieldName, parseValue(null, eventHeaders.get(fieldName)));
                } else {
                    final String mappedName = (def.getMappedName() == null) ? def.getFieldName() : def.getMappedName();
                    if (eventHeaders.containsKey(fieldName)) {
                        dbObject.put(mappedName, parseValue(def, eventHeaders.get(fieldName)));
                    }
                }
            }
        } else {
            for (FieldDefinition def : definition.getFields()) {
                final String fieldName = def.getFieldName();
                final String mappedName = (def.getMappedName() == null) ? def.getFieldName() : def.getMappedName();
                if (eventHeaders.containsKey(fieldName)) {
                    dbObject.put(mappedName, parseValue(def, eventHeaders.get(fieldName)));
                }
            }
        }

        return dbObject;
	}

	public List<DBObject> parse(List<Event> events) {
		List<DBObject> rows = new ArrayList<DBObject>(events.size());
		for (Event event : events) {
			rows.add(this.parse(event));
		}
		return rows;
	}

}
