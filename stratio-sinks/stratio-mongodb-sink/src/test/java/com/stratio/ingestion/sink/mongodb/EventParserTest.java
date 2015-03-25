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

import static org.fest.assertions.Assertions.assertThat;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.flume.event.EventBuilder;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Charsets;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSONParseException;

@RunWith(JUnit4.class)
public class EventParserTest {

    private FieldDefinition definition(MongoDataType type) {
        FieldDefinition def;
        switch (type) {
        case DATE:
            def = new DateFieldDefinition();
            break;
        case DOCUMENT:
            def = new DocumentFieldDefinition();
            break;
        default:
            def = new SimpleFieldDefinition(type);
        }

        return def;
    }

    @Test
    public void parseValue() {
        final EventParser eventParser = new EventParser();
        assertThat(eventParser.parseValue(definition(MongoDataType.STRING), "foo")).isEqualTo("foo");
        assertThat(eventParser.parseValue(definition(MongoDataType.INT32), "32")).isEqualTo(32);
        assertThat(eventParser.parseValue(definition(MongoDataType.INT64), "64")).isEqualTo(64L);
        assertThat(eventParser.parseValue(definition(MongoDataType.DOUBLE), "1.0")).isEqualTo(1.0);
        assertThat(eventParser.parseValue(definition(MongoDataType.BOOLEAN), "true")).isEqualTo(true);
        final DateTime now = DateTime.now().toDateTime(DateTimeZone.UTC);
        assertThat(eventParser.parseValue(definition(MongoDataType.DATE), Long.toString(now.getMillis()))).isEqualTo(
                now.toDate());
        assertThat((eventParser.parseValue(definition(MongoDataType.DATE), ISODateTimeFormat.dateTime().print(now))))
                .isEqualTo(
                        now.toDate());
        assertThat(eventParser.parseValue(definition(MongoDataType.NULL), "full")).isNull();
        assertThat(eventParser.parseValue(definition(MongoDataType.OBJECTID), "507c7f79bcf86cd7994f6c0e")).isEqualTo(
                new ObjectId("507c7f79bcf86cd7994f6c0e"));

        BasicDBList dbList = new BasicDBList();
        dbList.add(1);
        dbList.add(2);
        dbList.add(3);
        DBObject dbObject = new BasicDBObject();
        dbObject.put("abc", 123);
        dbObject.put("myArray", dbList);
        assertThat(eventParser.parseValue(definition(MongoDataType.OBJECT), "{ \"abc\": 123, \"myArray\": [1, 2, 3] }"))
                .isEqualTo(
                        dbObject);

        assertThat(eventParser.parseValue(definition(MongoDataType.BINARY), "U3RyYXRpbw==")).isEqualTo(
                "Stratio".getBytes(Charsets.UTF_8));

    }

    @Test
    public void parseValueForDate() {
        final EventParser eventParser = new EventParser();
        DateFieldDefinition fd = (DateFieldDefinition) definition(MongoDataType.DATE);
        fd.setDateFormat("yyyy/MM/dd");
        assertThat(eventParser.parseValue(fd, "2004/03/13")).isEqualTo(new Date(104, 2, 13));
    }

    @Test(expected = MongoSinkException.class)
    public void eventParserWithBadType() {
        new EventParser(MappingDefinition.load("/mapping_definition_bad_type.json"));
    }

    @Test
    public void parseRawBodyToRow() {
        final EventParser eventParser = new EventParser(MappingDefinition.load("/simple_body_row_raw.json"));
        assertThat(eventParser.parse(EventBuilder.withBody("TEST".getBytes(Charsets.UTF_8))).get("data")).isEqualTo(
                "TEST".getBytes(Charsets.UTF_8));
    }

    @Test
    public void parseJsonBodyToRow() {
        final EventParser eventParser = new EventParser(MappingDefinition.load("/simple_body_row_json.json"));
        assertThat(
                eventParser.parse(EventBuilder.withBody("{ \"foo\": \"bar\" }".getBytes(Charsets.UTF_8))).get("data"))
                .isEqualTo(
                        new BasicDBObject("foo", "bar"));
    }

    @Test(expected = JSONParseException.class)
    public void parseBadJsonBodyToRow() {
        final EventParser eventParser = new EventParser(MappingDefinition.load("/simple_body_row_json.json"));
        eventParser.parse(EventBuilder.withBody("{???? \"foo\": \"bar\" }".getBytes(Charsets.UTF_8)));
    }

    @Test
    public void parseWithoutMapping() {
        final EventParser eventParser = new EventParser(MappingDefinition.load("/full_map.json"));
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("myString", "\"bar\""); // Overwrites the value defined in JSON body
        headers.put("myInt64", "64");
        headers.put("myBoolean", "true");
        headers.put("myDouble", "1.0");
        headers.put("myNull", "\"foobar\"");
        headers.put("myObj", "{ \"foo\": \"bar\" }");
        headers.put("myArr", "[1,1.0,\"str\"]");
        DBObject dbObject = eventParser.parse(EventBuilder.withBody(new byte[0], headers));
        assertThat(dbObject.get("myString")).isEqualTo("bar");
        assertThat(dbObject.get("myInt64"))
                .isEqualTo(64); // XXX: If auto-mapped, 64 will be recognized as int32, not int64
        assertThat(dbObject.get("myBoolean")).isEqualTo(true);
        assertThat(dbObject.get("myDouble")).isEqualTo(1.0);
        assertThat(dbObject.get("myNull")).isEqualTo("foobar");
        assertThat(dbObject.get("myObj")).isEqualTo(new BasicDBObject("foo", "bar"));
        BasicDBList dbList = new BasicDBList();
        dbList.add(1);
        dbList.add(1.0);
        dbList.add("str");
        assertThat(dbObject.get("myArr")).isEqualTo(dbList);
    }

    @Test(expected = MongoSinkException.class)
    public void parseDocumentTypeWithNoValidSeparator() {
        EventParser eventParser = new EventParser();
        DBObject dbObject = buildExpectedObject();
        DocumentFieldDefinition fieldDefinition = (DocumentFieldDefinition) definition(MongoDataType.DOCUMENT);

        Map<String, FieldDefinition> documentMapping = new LinkedHashMap<String, FieldDefinition>();
        documentMapping.put("field1", new SimpleFieldDefinition(MongoDataType.STRING));
        documentMapping.put("field2", new SimpleFieldDefinition(MongoDataType.ARRAY));
        DocumentFieldDefinition fieldDefinition2 = (DocumentFieldDefinition) definition(MongoDataType.DOCUMENT);
        documentMapping.put("field3", fieldDefinition2);
        documentMapping.put("field6", new SimpleFieldDefinition(MongoDataType.STRING));
        Map<String, FieldDefinition> documentMapping2 = new LinkedHashMap<String, FieldDefinition>();
        documentMapping2.put("field4", new SimpleFieldDefinition(MongoDataType.STRING));
        documentMapping2.put("field5", new SimpleFieldDefinition(MongoDataType.ARRAY));
        fieldDefinition.setDocumentMapping(documentMapping);
        fieldDefinition2.setDocumentMapping(documentMapping2);

        assertThat(eventParser.parseValue(fieldDefinition, "Point1#[111.11,222.22]#Point2#[111.11,222.22]#Point3"))
                .isEqualTo(
                        dbObject);
    }

    @Test
    public void parseDocumentTypeWithValidSeparator() {
        EventParser eventParser = new EventParser();
        DBObject dbObject = buildExpectedObject();
        DocumentFieldDefinition fieldDefinition = (DocumentFieldDefinition) definition(MongoDataType.DOCUMENT);
        fieldDefinition.setDelimiter("#");
        Map<String, FieldDefinition> documentMapping = new LinkedHashMap<String, FieldDefinition>();
        documentMapping.put("field1", new SimpleFieldDefinition(MongoDataType.STRING));
        documentMapping.put("field2", new SimpleFieldDefinition(MongoDataType.ARRAY));
        DocumentFieldDefinition fieldDefinition2 = (DocumentFieldDefinition) definition(MongoDataType.DOCUMENT);
        fieldDefinition2.setDelimiter("#");
        documentMapping.put("field3", fieldDefinition2);
        documentMapping.put("field6", new SimpleFieldDefinition(MongoDataType.STRING));
        Map<String, FieldDefinition> documentMapping2 = new LinkedHashMap<String, FieldDefinition>();
        documentMapping2.put("field4", new SimpleFieldDefinition(MongoDataType.STRING));
        documentMapping2.put("field5", new SimpleFieldDefinition(MongoDataType.ARRAY));
        fieldDefinition.setDocumentMapping(documentMapping);
        fieldDefinition2.setDocumentMapping(documentMapping2);

        assertThat(eventParser.parseValue(fieldDefinition, "Point1#[111.11,222.22]#Point2#[111.11,222.22]#Point3"))
                .isEqualTo(
                        dbObject);
    }

    @Test
    public void parseFieldMappedFromEmbeddedObject() {

        EventParser eventParser = new EventParser(MappingDefinition.load("/mapping_definition_with_embedded_object"
                + ".json"));

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("field1", "\"value1\"");
        headers.put("object", "{\"field1\": \"embeddedValue1\", \"field2\": 2, \"embeddedObject\": { "
                + "\"field3\": \"embeddedValue2\"}}");

        final DBObject objectParsed = eventParser.parse(EventBuilder.withBody(new byte[0], headers));
        assertThat(objectParsed.get("mappedField1")).isEqualTo("embeddedValue2");

    }

    @Test
    public void parseFieldMappedFromNoValidEmbeddedObject() {

        EventParser eventParser = new EventParser(MappingDefinition.load
                ("/mapping_definition_with_no_valid_embedded_object"
                        + ".json"));

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("field1", "\"value1\"");
        headers.put("object", "{\"field1\": \"embeddedValue1\", \"field2\": 2, \"embeddedObject\": { "
                + "\"field3\": \"embeddedValue2\"}}");

        final DBObject objectParsed = eventParser.parse(EventBuilder.withBody(new byte[0], headers));
        assertThat(objectParsed.get("mappedField1")).isNull();

    }

    @Test
    public void parseSubFieldMappedFromInteger() {

        EventParser eventParser = new EventParser(MappingDefinition.load
                ("/mapping_definition_with_no_valid_embedded_object_2"
                        + ".json"));

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("field1", "\"value1\"");
        headers.put("field2", "100");
        headers.put("object", "{\"subfield1\": \"embeddedValue1\", \"subfield2\": 2, \"embeddedObject\": { "
                + "\"subfield3\": \"embeddedValue2\"}}");

        final DBObject objectParsed = eventParser.parse(EventBuilder.withBody(new byte[0], headers));
        assertThat(objectParsed.get("mappedField1")).isNull();

    }

    private DBObject buildExpectedObject() {
        DBObject dbObject = new BasicDBObject();
        BasicDBObject object2 = new BasicDBObject();
        BasicDBList locList = new BasicDBList();
        locList.add(111.11);
        locList.add(222.22);
        dbObject.put("field1", "Point1");
        dbObject.put("field2", locList);
        dbObject.put("field3", object2);
        dbObject.put("field6", "Point3");

        object2.put("field4", "Point2");
        object2.put("field5", locList);

        return dbObject;
    }

}
