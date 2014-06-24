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

import com.google.common.base.Charsets;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSONParseException;
import org.apache.flume.event.EventBuilder;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(JUnit4.class)
public class EventParserTest {

    private FieldDefinition definition(MongoDataType type) {
        FieldDefinition def = new FieldDefinition();
        def.setType(type);
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
        assertThat(eventParser.parseValue(definition(MongoDataType.DATE), Long.toString(now.getMillis()))).isEqualTo(now.toDate());
        assertThat((eventParser.parseValue(definition(MongoDataType.DATE), ISODateTimeFormat.dateTime().print(now)))).isEqualTo(now.toDate());
        assertThat(eventParser.parseValue(definition(MongoDataType.NULL), "full")).isNull();
        assertThat(eventParser.parseValue(definition(MongoDataType.OBJECTID), "507c7f79bcf86cd7994f6c0e")).isEqualTo(new ObjectId("507c7f79bcf86cd7994f6c0e"));

        BasicDBList dbList = new BasicDBList();
        dbList.add(1);
        dbList.add(2);
        dbList.add(3);
        DBObject dbObject = new BasicDBObject();
        dbObject.put("abc", 123);
        dbObject.put("myArray", dbList);
        assertThat(eventParser.parseValue(definition(MongoDataType.OBJECT), "{ \"abc\": 123, \"myArray\": [1, 2, 3] }"))
                .isEqualTo(dbObject);

        assertThat(eventParser.parseValue(definition(MongoDataType.BINARY), "U3RyYXRpbw=="))
                .isEqualTo("Stratio".getBytes(Charsets.UTF_8));
    }

    @Test
    public void parseValueForDate() {
        final EventParser eventParser = new EventParser();
        FieldDefinition fd = definition(MongoDataType.DATE);
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
        assertThat(eventParser.parse(EventBuilder.withBody("TEST".getBytes(Charsets.UTF_8))).get("data"))
                .isEqualTo("TEST".getBytes(Charsets.UTF_8));
    }

    @Test
    public void parseJsonBodyToRow() {
        final EventParser eventParser = new EventParser(MappingDefinition.load("/simple_body_row_json.json"));
        assertThat(eventParser.parse(EventBuilder.withBody("{ \"foo\": \"bar\" }".getBytes(Charsets.UTF_8))).get("data"))
                .isEqualTo(new BasicDBObject("foo", "bar"));
    }

    @Test(expected = JSONParseException.class)
    public void parseBadJsonBodyToRow() {
        final EventParser eventParser = new EventParser(MappingDefinition.load("/simple_body_row_json.json"));
        eventParser.parse(EventBuilder.withBody("{???? \"foo\": \"bar\" }".getBytes(Charsets.UTF_8)));
    }

    @Test
    public void parseWithoutMapping() {
        final EventParser eventParser = new EventParser(MappingDefinition.load("/full_map.json"));
        Map<String,String> headers = new HashMap<>();
        headers.put("myString", "\"bar\""); // Overwrites the value defined in JSON body
        headers.put("myInt64", "64");
        headers.put("myBoolean", "true");
        headers.put("myDouble", "1.0");
        headers.put("myNull", "\"foobar\"");
        headers.put("myObj", "{ \"foo\": \"bar\" }");
        headers.put("myArr", "[1,1.0,\"str\"]");
        DBObject dbObject = eventParser.parse(EventBuilder.withBody(new byte[0], headers));
        assertThat(dbObject.get("myString")).isEqualTo("bar");
        assertThat(dbObject.get("myInt64")).isEqualTo(64); //XXX: If auto-mapped, 64 will be recognized as int32, not int64
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

}
