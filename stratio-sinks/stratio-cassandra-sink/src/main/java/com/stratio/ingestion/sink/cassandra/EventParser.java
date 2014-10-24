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

import com.google.common.base.Charsets;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Event;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.format.ISODateTimeFormat;

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
		List<CassandraField<?>> fields = new ArrayList<CassandraField<?>>();
		
		for (FieldDefinition def : this.definition.getFields()) {
			if (event.getHeaders().containsKey(def.getColumnName())) {
				fields.add(parseField(event.getHeaders().get(def.getColumnName()), def));
			} else if (def.getColumnName().equals(BODY_COLUMN)) {
				fields.add(parseField(new String(event.getBody(), Charsets.UTF_8), def));
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

	private static Date parseDate(String rawValue, String dateFormat) {
        if (dateFormat == null) {
            if (StringUtils.isNumeric(rawValue)) {
                return new Date(Long.parseLong(rawValue));
            } else {
                return ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(rawValue).toDate();
            }
        }
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		try {
			return sdf.parse(rawValue);
		} catch (ParseException e) {
			throw new CassandraSinkException(e);
		}
	}

	private static InetAddress parseInetSocketAddress(String field) {
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
        switch (type) {
            case COUNTER:
            case VARINT:
            case INT:
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                rawValue = rawValue.replaceAll("\\s+", "");
                break;
        }

        switch (type) {
            case ASCII: //FIXME: What about non-ASCII?
            case TEXT:  //FIXME: This is expected to be UTF-8
            case VARCHAR: //FIXME: This is expected to be UTF-8
                return rawValue;
            case TIMESTAMP:
                return parseDate(rawValue, dateFormat);
            case UUID:
            case TIMEUUID:
                return UUID.fromString(rawValue);
            case COUNTER:
                return Long.valueOf(rawValue);
            case VARINT:
                return new BigInteger(rawValue);
            case INT:
                return Integer.valueOf(rawValue);
            case BIGINT:
                return new BigInteger(rawValue);
            case FLOAT:
                return Float.valueOf(rawValue);
            case DOUBLE:
                return Double.valueOf(rawValue);
            case DECIMAL:
                return new BigDecimal(rawValue);
            case BOOLEAN:
                return Boolean.valueOf(rawValue);
            case INET:
                return parseInetSocketAddress(rawValue);
            default:
                throw new CassandraSinkException(
                        "Cassandra type not supported: " + type.toString()
                );
        }
	}
}
