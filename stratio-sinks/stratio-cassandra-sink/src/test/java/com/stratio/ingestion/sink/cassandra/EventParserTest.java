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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import static org.fest.assertions.Assertions.assertThat;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.DataType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class EventParserTest {

	private EventParser instance;

	@Before
	public void before() throws IOException {
		this.instance = new EventParser(IOUtils.toString(this.getClass()
				.getResourceAsStream("/definition.json")));
	}

	@Test
	public void shouldParseDate() throws Exception {
        DateTime now = DateTime.now();
		assertThat(
                EventParser.parseValue(ISODateTimeFormat.dateTime().print(now),
                        DataType.Name.TIMESTAMP, null))
                .isEqualTo(now.toDate());
        assertThat(
                EventParser.parseValue(ISODateTimeFormat.dateTimeNoMillis().print(now),
                        DataType.Name.TIMESTAMP, null))
                .isEqualTo(now.withMillisOfSecond(0).toDate());
        assertThat(
                EventParser.parseValue(Long.toString(now.getMillis()),
                        DataType.Name.TIMESTAMP, null))
                .isEqualTo(now.toDate());
        assertThat(
                EventParser.parseValue("10/12/2004 12:01:02",
                        DataType.Name.TIMESTAMP, "dd/MM/yyyy HH:mm:ss"))
                .isEqualTo(new DateTime(2004, 12, 10, 12, 1, 2, 0).toDate());
	}

	@Test
	public void shouldParsePrimitiveTypes() throws Exception {
		Object integer = EventParser.parseValue("1", DataType.Name.INT, "");
        assertThat(integer)
                .isInstanceOf(Integer.class)
                .isEqualTo(1);
        integer = EventParser.parseValue(Integer.toString(Integer.MAX_VALUE), DataType.Name.INT, "");
        assertThat(integer)
                .isInstanceOf(Integer.class)
                .isEqualTo(Integer.MAX_VALUE);
        integer = EventParser.parseValue(Integer.toString(Integer.MIN_VALUE), DataType.Name.INT, "");
        assertThat(integer)
                .isInstanceOf(Integer.class)
                .isEqualTo(Integer.MIN_VALUE);
        integer = EventParser.parseValue(" 1 2 ", DataType.Name.INT, "");
        assertThat(integer)
                .isInstanceOf(Integer.class)
                .isEqualTo(12);

        Object counter = EventParser.parseValue("1", DataType.Name.COUNTER, "");
        assertThat(counter)
                .isEqualTo(1L);
        counter = EventParser.parseValue(Long.toString(Long.MAX_VALUE), DataType.Name.COUNTER, "");
        assertThat(counter)
                .isEqualTo(Long.MAX_VALUE);
        counter = EventParser.parseValue(Long.toString(Long.MIN_VALUE), DataType.Name.COUNTER, "");
        assertThat(counter)
                .isEqualTo(Long.MIN_VALUE);
        counter = EventParser.parseValue(" 1 2 ", DataType.Name.COUNTER, "");
        assertThat(counter)
                .isEqualTo(12L);

        Object _float = EventParser.parseValue("1", DataType.Name.FLOAT, "");
        assertThat(_float)
                .isInstanceOf(Float.class)
                .isEqualTo(1f);
        _float = EventParser.parseValue("1.0", DataType.Name.FLOAT, "");
        assertThat(_float)
                .isInstanceOf(Float.class)
                .isEqualTo(1f);
        _float = EventParser.parseValue(Float.toString(Float.MAX_VALUE), DataType.Name.FLOAT, "");
        assertThat(_float)
                .isInstanceOf(Float.class)
                .isEqualTo(Float.MAX_VALUE);
        _float = EventParser.parseValue(Float.toString(Float.MIN_VALUE), DataType.Name.FLOAT, "");
        assertThat(_float)
                .isInstanceOf(Float.class)
                .isEqualTo(Float.MIN_VALUE);
        _float = EventParser.parseValue(" 1 . 0 ", DataType.Name.FLOAT, "");
        assertThat(_float)
                .isInstanceOf(Float.class)
                .isEqualTo(1f);

        Object _double = EventParser.parseValue("1", DataType.Name.DOUBLE, "");
        assertThat(_double)
                .isInstanceOf(Double.class)
                .isEqualTo(1.0);
        _double = EventParser.parseValue("0", DataType.Name.DOUBLE, "");
        assertThat(_double)
                .isInstanceOf(Double.class)
                .isEqualTo(0.0);
        _double = EventParser.parseValue(Double.toString(Double.MAX_VALUE), DataType.Name.DOUBLE, "");
        assertThat(_double)
                .isInstanceOf(Double.class)
                .isEqualTo(Double.MAX_VALUE);
        _double = EventParser.parseValue(Double.toString(Double.MIN_VALUE), DataType.Name.DOUBLE, "");
        assertThat(_double)
                .isInstanceOf(Double.class)
                .isEqualTo(Double.MIN_VALUE);
        _double = EventParser.parseValue(" 1 . 0 ", DataType.Name.DOUBLE, "");
        assertThat(_double)
                .isInstanceOf(Double.class)
                .isEqualTo(1.0);

        for (DataType.Name type : Arrays.asList(DataType.Name.BIGINT, DataType.Name.VARINT)) {
            Object bigInteger = EventParser.parseValue("1", type, "");
            assertThat(bigInteger)
                    .isInstanceOf(BigInteger.class)
                    .isEqualTo(BigInteger.valueOf(1));
            bigInteger = EventParser.parseValue("0", type, "");
            assertThat(bigInteger)
                    .isInstanceOf(BigInteger.class)
                    .isEqualTo(BigInteger.valueOf(0));
            bigInteger = EventParser.parseValue(
                    BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)).toString(),
                    type, "");
            assertThat(bigInteger)
                    .isInstanceOf(BigInteger.class)
                    .isEqualTo(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)));
            bigInteger = EventParser.parseValue(
                    BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2)).toString(),
                    type, "");
            assertThat(bigInteger)
                    .isInstanceOf(BigInteger.class)
                    .isEqualTo(BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2)));
            bigInteger = EventParser.parseValue(" 1 2 ", type, "");
            assertThat(bigInteger)
                    .isInstanceOf(BigInteger.class)
                    .isEqualTo(BigInteger.valueOf(12));
        }

        Object bigDecimal = EventParser.parseValue("1", DataType.Name.DECIMAL, "");
        assertThat(bigDecimal)
                .isInstanceOf(BigDecimal.class)
                .isEqualTo(BigDecimal.valueOf(1));
        bigDecimal = EventParser.parseValue("0", DataType.Name.DECIMAL, "");
        assertThat(bigDecimal)
                .isInstanceOf(BigDecimal.class)
                .isEqualTo(BigDecimal.valueOf(0));
        bigDecimal = EventParser.parseValue(
                BigDecimal.valueOf(Double.MAX_VALUE).multiply(BigDecimal.valueOf(2)).toString(),
                DataType.Name.DECIMAL, "");
        assertThat(bigDecimal)
                .isInstanceOf(BigDecimal.class)
                .isEqualTo(BigDecimal.valueOf(Double.MAX_VALUE).multiply(BigDecimal.valueOf(2)));
        bigDecimal = EventParser.parseValue(
                BigDecimal.valueOf(Double.MIN_VALUE).multiply(BigDecimal.valueOf(2)).toString(),
                DataType.Name.DECIMAL, "");
        assertThat(bigDecimal)
                .isInstanceOf(BigDecimal.class)
                .isEqualTo(BigDecimal.valueOf(Double.MIN_VALUE).multiply(BigDecimal.valueOf(2)));
        bigDecimal = EventParser.parseValue(" 1 2 ", DataType.Name.DECIMAL, "");
        assertThat(bigDecimal)
                .isInstanceOf(BigDecimal.class)
                .isEqualTo(BigDecimal.valueOf(12));

        Object string = EventParser.parseValue("string", DataType.Name.TEXT, "");
        assertThat(string)
                .isInstanceOf(String.class)
                .isEqualTo("string");

		Object bool = EventParser.parseValue("true", DataType.Name.BOOLEAN, "");
        assertThat(bool)
                .isInstanceOf(Boolean.class)
                .isEqualTo(true);

        Object addr = EventParser.parseValue("192.168.1.1", DataType.Name.INET, "");
        assertThat(addr)
                .isInstanceOf(InetAddress.class)
                .isEqualTo(InetAddress.getByName("192.168.1.1"));

        UUID randomUUID = UUID.randomUUID();
        Object uuid = EventParser.parseValue(randomUUID.toString(), DataType.Name.UUID, "");
        assertThat(uuid)
                .isInstanceOf(UUID.class)
                .isEqualTo(randomUUID);
	}

	@Test
	public void shouldParseLists() throws Exception {
		String rawList = "1;2;3";
		CassandraField<List> listField = EventParser.parseList(rawList,
				this.instance.getDefinition().getFields().get(1));
		Assert.assertEquals(listField.getValue().get(0), 1);
	}

    @Test
    public void shouldParseSets() throws Exception {
        String rawSet = "1;1;2;3";
        FieldDefinition fieldDefinition = new FieldDefinition();
        fieldDefinition.setItemSeparator(";");
        fieldDefinition.setListValueType("INT");
        fieldDefinition.setType("SET");
        HashSet<Integer> set = Sets.newHashSet(1, 2, 3);
        assertThat(EventParser.parseField(rawSet, fieldDefinition).getValue())
                .isEqualTo(set);
    }

	@Test
	public void shouldParseMap() throws Exception {
		String rawMap = "1:192.168.1.1;2:192.168.1.2;3:192.168.1.3";
		CassandraField<Map> mapField = EventParser.parseMap(rawMap,
				this.instance.getDefinition().getFields().get(2));
		Map<Integer, InetAddress> expected = new HashMap<Integer, InetAddress>();
		expected.put(1, InetAddress.getByName("192.168.1.1"));
		Assert.assertEquals(expected.get(1), mapField.getValue().get(1));
	}

    @Test
    public void shouldParseEventList() throws Exception {
        List<Event> events = Arrays.asList(
                EventBuilder.withBody(new byte[0], ImmutableMap.of("int_field", "1")),
                EventBuilder.withBody(new byte[0], ImmutableMap.of("text_field", "string"))
        );
        EventParser eventParser = new EventParser(IOUtils.toString(this.getClass()
                .getResourceAsStream("/definitionAllTypes.json")));
        List<CassandraRow> parsedEvents = eventParser.parse(events);
        assertThat(parsedEvents).isEqualTo(
                Arrays.asList(
                    new CassandraRow(Arrays.<CassandraField<?>>asList(new CassandraField<Integer>("int_field", 1))),
                    new CassandraRow(Arrays.<CassandraField<?>>asList(new CassandraField<String>("text_field", "string")))
                )
        );
    }

}
