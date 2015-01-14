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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Event;
import org.joda.time.format.ISODateTimeFormat;

import com.datastax.driver.core.DataType;
import com.google.common.base.Charsets;

class EventParser {

	private final ColumnDefinition definition;
	private final String bodyColumn;
	
	public EventParser(final ColumnDefinition definition, final String bodyColumn) {
		this.definition = definition;
		this.bodyColumn = bodyColumn;
	}
	
	@SuppressWarnings("rawtypes")
	public CassandraRow parse(final Event event) {
		final List<CassandraField<?>> fields = new ArrayList<CassandraField<?>>();
		for (final FieldDefinition def : this.definition.getFields()) {
			if (event.getHeaders().containsKey(def.getColumnName())) {
				fields.add(parseField(event.getHeaders().get(def.getColumnName()), def));
			} else if (def.getColumnName().equals(bodyColumn)) {
				//TODO: Ideally body should be written directly as byte[] to BLOB
				fields.add(parseField(new String(event.getBody(), Charsets.UTF_8), def));
			}
		}
		return new CassandraRow(fields);
	}

	public List<CassandraRow> parse(final List<Event> events) {
		final List<CassandraRow> rows = new ArrayList<CassandraRow>(events.size());
		for (final Event event : events) {
			rows.add(this.parse(event));
		}
		return rows;
	}

	public ColumnDefinition getDefinition() {
		return this.definition;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static CassandraField parseField(final String fieldValue, final FieldDefinition definition) {
		final String columnName = definition.getColumnName();
		final DataType.Name dataTypeName = DataType.Name.valueOf(definition.getType());
		if (DataType.Name.SET.equals(dataTypeName)) {
			return parseSet(fieldValue, definition);
		} else if (DataType.Name.MAP.equals(dataTypeName)) {
			return parseMap(fieldValue, definition);
		} else if (DataType.Name.LIST.equals(dataTypeName)) {
			return parseList(fieldValue, definition);
		} else {
			final Object value = parseValue(fieldValue, dataTypeName);
			return new CassandraField(columnName, value);
		}
	}

	static CassandraField<Map<Object,Object>> parseMap(final String fieldValue, final FieldDefinition definition) {
		final List<String> rawItems = Arrays.asList(fieldValue.split(definition.getItemSeparator()));

		final Map<Object,Object> map = new HashMap<Object,Object>();
		for (final String rawItem : rawItems) {
			final String[] fields = rawItem.split(definition.getMapValueSeparator());
			final String rawKey = fields[0];
			final String rawValue = fields[1];
			final Object key = parseValue(rawKey, DataType.Name.valueOf(definition.getMapKeyType()));
			final Object value = parseValue(rawValue, DataType.Name.valueOf(definition.getMapValueType()));
			map.put(key, value);
		}
		return new CassandraField<Map<Object,Object>>(definition.getColumnName(), map);
	}

	private static Date parseDate(final String rawValue) {
		if (StringUtils.isNumeric(rawValue)) {
			return new Date(Long.parseLong(rawValue));
		} else {
			return ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(rawValue).toDate();
		}
	}

	private static InetAddress parseInetSocketAddress(final String fieldValue) {
		try {
			return InetAddress.getByName(fieldValue);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}
	}

	static CassandraField<Set<Object>> parseSet(final String fieldValue, final FieldDefinition definition) {
		Set<Object> items = new HashSet<Object>();
		List<String> rawItems = Arrays.asList(fieldValue.split(definition.getItemSeparator()));
		for (String rawItem : rawItems) {
			items.add(parseValue(rawItem, DataType.Name.valueOf(definition.getListValueType())));
		}
		return new CassandraField<Set<Object>>(definition.getColumnName(), items);
	}

	static CassandraField<List<Object>> parseList(final String field, final FieldDefinition definition) {
		final List<Object> items = new ArrayList<Object>();
		final List<String> rawItems = Arrays.asList(field.split(definition
				.getItemSeparator()));
		for (final String rawItem : rawItems) {
			items.add(parseValue(rawItem, DataType.Name.valueOf(definition.getListValueType())));
		}
		return new CassandraField<List<Object>>(definition.getColumnName(), items);
	}

	static Object parseValue(String rawValue, final DataType.Name type) {
				// Type-dependent input sanitization
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
                return parseDate(rawValue);
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
                return Long.valueOf(rawValue);
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
                throw new IllegalArgumentException("Cassandra type not supported: " + type);
        }
	}
}
