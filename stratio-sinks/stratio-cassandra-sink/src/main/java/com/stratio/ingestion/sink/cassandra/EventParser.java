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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.EnumUtils;
import org.apache.flume.Event;
import org.codehaus.jackson.map.ObjectMapper;

import com.datastax.driver.core.DataType;

class EventParser {

	private static final String BODY_COLUMN = "data";
	private final ColumnDefinition definition;

	public EventParser(String jsonDefinition) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			this.definition = mapper.readValue(jsonDefinition,
					ColumnDefinition.class);
			validateDefinition();
		} catch (Exception e) {
			throw new CassandraSinkException(e);
		}
	}
	
	public EventParser(ColumnDefinition definition) {
		this.definition = definition;
	}
	
	private void validateDefinition() {
		for (FieldDefinition field : definition.getFields()) {
			boolean validEnum = EnumUtils.isValidEnum(DataType.Name.class, field.getType());
			if (!validEnum)
				throw new CassandraSinkException("Field type \"" + field.getType() + "\" is not a valid one.");
		}
	}
	
	@SuppressWarnings("rawtypes")
	public CassandraRow parse(Event event) {
		List<CassandraField> fields = new ArrayList<CassandraField>();
		
		for (FieldDefinition def : this.definition.getFields()) {
			if (event.getHeaders().containsKey(def.getColumnName())) {
				fields.add(parseField(event.getHeaders().get(def.getColumnName()), def));
			} else if (def.getColumnName().equals(BODY_COLUMN)) {
				fields.add(parseField(new String(event.getBody()), def));
			}
		}

		return new CassandraRow(fields);
	}

	public List<CassandraRow> parse(List<Event> events) {
		List<CassandraRow> rows = new ArrayList<CassandraRow>(events.size());
		for (Event event : events) {
			rows.add(this.parse(event));
		}
		return rows;
	}

	public ColumnDefinition getDefinition() {
		return this.definition;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static final CassandraField parseField(String field,
			FieldDefinition definition) {
		String columnName = definition.getColumnName();
		if (DataType.Name.valueOf(definition.getType()).equals(
				DataType.Name.SET)) {
			return parseSet(field, definition);
		} else if (DataType.Name.valueOf(definition.getType()).equals(
				DataType.Name.MAP)) {
			return parseMap(field, definition);
		} else if (DataType.Name.valueOf(definition.getType()).equals(
				DataType.Name.LIST)) {
			return parseList(field, definition);
		} else {
			return new CassandraField(columnName, parseValue(field,
					DataType.Name.valueOf(definition.getType()),
					definition.getDateFormat()));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	final static CassandraField<Map> parseMap(String field,
			FieldDefinition definition) {
		List<String> rawItems = Arrays.asList(field.split(definition
				.getItemSeparator()));
		Map map = new HashMap();
		for (String rawItem : rawItems) {
			String rawKey = rawItem.split(definition.getMapValueSeparator())[0];
			String rawValue = rawItem.split(definition.getMapValueSeparator())[1];
			Object key = parseValue(rawKey,
					DataType.Name.valueOf(definition.getMapKeyType()),
					definition.getDateFormat());
			Object value = parseValue(rawValue,
					DataType.Name.valueOf(definition.getMapValueType()),
					definition.getDateFormat());
			map.put(key, value);
		}
		return new CassandraField<Map>(definition.getColumnName(), map);
	}

	final static CassandraField<Date> parseDateField(String field,
			String columnName, String dateFormat) {
		return new CassandraField<Date>(columnName,
				parseDate(field, dateFormat));
	}

	final static Date parseDate(String rawValue, String dateFormat) {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		try {
			return sdf.parse(rawValue);
		} catch (ParseException e) {
			throw new CassandraSinkException(e);
		}
	}

	final static InetAddress parseInetSocketAddress(String field) {
		try {
			return InetAddress.getByName(field);
		} catch (UnknownHostException e) {
			throw new CassandraSinkException(e);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	final static CassandraField<Set> parseSet(String field,
			FieldDefinition definition) {
		Set items = new HashSet();
		List<String> rawItems = Arrays.asList(field.split(definition
				.getItemSeparator()));
		for (String rawItem : rawItems) {
			items.add(parseValue(rawItem,
					DataType.Name.valueOf(definition.getListValueType()),
					definition.getDateFormat()));
		}
		return new CassandraField<Set>(definition.getColumnName(), items);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	final static CassandraField<List> parseList(String field,
			FieldDefinition definition) {
		List items = new ArrayList();
		List<String> rawItems = Arrays.asList(field.split(definition
				.getItemSeparator()));
		for (String rawItem : rawItems) {
			items.add(parseValue(rawItem,
					DataType.Name.valueOf(definition.getListValueType()),
					definition.getDateFormat()));
		}
		return new CassandraField<List>(definition.getColumnName(), items);
	}

	final static Object parseValue(String rawValue, DataType.Name type,
			String dateFormat) {
		if (type.equals(DataType.Name.DECIMAL)) {
			return BigDecimal.valueOf(Double.parseDouble(rawValue.replaceAll(
					"\\s+", "")));
		} else if (type.equals(DataType.Name.ASCII)) {
			return rawValue;
		} else if (type.equals(DataType.Name.VARCHAR)) {
			return rawValue;
		} else if (type.equals(DataType.Name.COUNTER)) {
			return Long.parseLong(rawValue.replaceAll("\\s+", ""));
		} else if (type.equals(DataType.Name.VARINT)) {
			return new BigInteger(rawValue.replaceAll("\\s+", ""));
		} else if (type.equals(DataType.Name.BIGINT)) {
			return Long.parseLong(rawValue.replaceAll("\\s+", ""));
		} else if (type.equals(DataType.Name.BOOLEAN)) {
			return new Boolean(Boolean.parseBoolean(rawValue));
		} else if (type.equals(DataType.Name.TIMESTAMP)) {
			return parseDate(rawValue, dateFormat);
		} else if (type.equals(DataType.Name.DOUBLE)) {
			return new Double(Double.parseDouble(rawValue
					.replaceAll("\\s+", "")));
		} else if (type.equals(DataType.Name.FLOAT)) {
			return new Float(Float.parseFloat(rawValue.replaceAll("\\s+", "")));
		} else if (type.equals(DataType.Name.INET)) {
			return parseInetSocketAddress(rawValue);
		} else if (type.equals(DataType.Name.INT)) {
			return Integer.parseInt(rawValue.replaceAll("\\s+", ""));
		} else if (type.equals(DataType.Name.TEXT)) {
			return rawValue;
		} else if (type.equals(DataType.Name.UUID)) {
			return UUID.fromString(rawValue);
		}
		throw new CassandraSinkException("Class not found for type: "
				+ type.toString());
	}
}
