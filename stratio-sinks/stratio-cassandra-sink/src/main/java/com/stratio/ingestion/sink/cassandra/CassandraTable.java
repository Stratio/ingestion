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

import static com.stratio.ingestion.sink.cassandra.CassandraUtils.parseValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Charsets;

class CassandraTable {

  private static final Logger log = LoggerFactory.getLogger(CassandraTable.class);

  private final Session session;
  private final TableMetadata table;
  private final ConsistencyLevel consistencyLevel;
  private final String bodyColumn;

  private final List<ColumnMetadata> columns;
  private final int totalColumns;
  private final List<String> primaryKeys;

  private final boolean ignoreCase;

  public CassandraTable(
      final Session session,
      final TableMetadata table,
      final ConsistencyLevel consistencyLevel,
      final String bodyColumn) {
    this(session, table, consistencyLevel, bodyColumn, false);

  }

  public CassandraTable(
      final Session session,
      final TableMetadata table,
      final ConsistencyLevel consistencyLevel,
      final String bodyColumn,
      final boolean ignoreCase) {
    this.session = session;
    this.table = table;
    this.consistencyLevel = consistencyLevel;
    this.bodyColumn = bodyColumn;

    this.columns = table.getColumns();
    this.totalColumns = this.columns.size();
    this.primaryKeys = new ArrayList<String>();
    for (final ColumnMetadata column : table.getPrimaryKey()) {
      primaryKeys.add(column.getName());
    }

    this.ignoreCase = ignoreCase;
  }

  public void save(final List<Event> events) {
    final BatchStatement batch = new BatchStatement();
    for (final Event event : events) {
      final Map<String, Object> parsedEvent = parse(event);
      if (parsedEvent.isEmpty()) {
        log.warn("Event {} could not be mapped. Suggestion: Cassandra is case sensitive, so maybe you can check field names.", event);
        continue;
      }
      if (!hasPrimaryKey(parsedEvent)) {
        break;
      }
      final Insert insert = QueryBuilder.insertInto(table);
      for (final Map.Entry<String, Object> entry : parsedEvent.entrySet()) {
        insert.value(entry.getKey(), entry.getValue());
      }
      if (log.isTraceEnabled()) {
        log.trace("Preparing insert for table {}: {}", table.getName(), insert.getQueryString());
      }
      batch.add(insert);
    }
    if (batch.getStatements().isEmpty()) {
      log.warn("No event produced insert query for table {}", table.getName());
      return;
    }
    batch.setConsistencyLevel(consistencyLevel);
    session.execute(batch);
  }

  private boolean hasPrimaryKey(final Map<String, Object> parsedEvent) {
    for (final String primaryKey : primaryKeys) {
      if (!parsedEvent.containsKey(primaryKey)) {
        log.info("Event {} misses primary key ({}), skipping", parsedEvent, primaryKey);
        return false;
      }
    }
    return true;
  }

  public Map<String, Object> parse(final Event event) {
    // translate to lowercase for ignorecase option
    final Map<String, String> headers = ignoreCase ? processHeadersIgnoreCase(event.getHeaders())
        : event.getHeaders();
    final int maxValues = Math.min(headers.size(), totalColumns);
    final Map<String, Object> result = new HashMap<String, Object>(maxValues);

    for (final ColumnMetadata column : columns) {
      final String columnName = ignoreCase ? column.getName().toLowerCase() : column.getName();

      if (headers.containsKey(columnName) && !columnName.equals(bodyColumn)) {
        result.put(columnName, parseValue(column.getType(), headers.get(columnName)));
      } else if (columnName.equals(bodyColumn)) {
        result.put(columnName, parseValue(column.getType(), new String(event.getBody(), Charsets.UTF_8)));
      }
    }

    return result;
  }

  private Map<String, String> processHeadersIgnoreCase(Map<String, String> headers) {

    Map<String, String> headersLowerCase = new HashMap<>(headers.size());

    Iterator<Entry<String, String>> iter = headers.entrySet().iterator();
    Entry<String, String> entry;
    String keyInLowerCase;
    while (iter.hasNext()) {
      entry = iter.next();
      keyInLowerCase = entry.getKey().toLowerCase();
      headersLowerCase.put(keyInLowerCase, entry.getValue());
    }

    return headersLowerCase;

  }

}
