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

import java.util.List;

import org.apache.flume.Event;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

class CassandraTable {

  private final Session session;
  private final String keyspace;
  private final String table;
  private final EventParser parser;
  private final ConsistencyLevel consistencyLevel;

  public CassandraTable(final Session session, final String keyspace, final String table,
      final EventParser parser, final ConsistencyLevel consistencyLevel) {
    this.session = session;
    this.keyspace = keyspace;
    this.table = table;
    this.parser = parser;
    this.consistencyLevel = consistencyLevel;
  }

  public void save(final List<Event> events) {
    final List<CassandraRow> rows = this.parser.parse(events);
    final BatchStatement batch = new BatchStatement();
    for (final CassandraRow row : rows) {
      final Insert buildInsert = buildInsert(row, this.keyspace, this.table);
      batch.add(buildInsert);
    }
    batch.setConsistencyLevel(consistencyLevel);
    this.session.execute(batch);
  }

  @SuppressWarnings("rawtypes")
  private static Insert buildInsert(final CassandraRow row, final String keyspace, final String table) {
    final Insert insert = QueryBuilder.insertInto(keyspace, table);
    for (final CassandraField field : row.getFields()) {
      insert.value(field.getColumnName(), field.getValue());
    }
    return insert;
  }

}
