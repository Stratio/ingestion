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

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class TestCassandraTable {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  Logger log = LoggerFactory.getLogger(TestCassandraTable.class);
  @Mock
  Session session;

  @Mock
  TableMetadata tableMetadata;

  @Before
  public void setup() {
    session = mock(Session.class);
    tableMetadata = mock(TableMetadata.class);

  }

  private void mockTableMetadata() {
    final ColumnMetadata idColumn = mock(ColumnMetadata.class);
    when(idColumn.getName()).thenReturn("id");
    when(idColumn.getType()).thenReturn(DataType.cint());

    final ColumnMetadata textColumn = mock(ColumnMetadata.class);
    when(textColumn.getName()).thenReturn("text_col");
    when(textColumn.getType()).thenReturn(DataType.text());

    final KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
    when(keyspaceMetadata.getName()).thenReturn("my_keyspace");

    when(tableMetadata.getName()).thenReturn("my_table");
    when(tableMetadata.getColumns()).thenReturn(ImmutableList.of(idColumn, textColumn));
    when(tableMetadata.getKeyspace()).thenReturn(keyspaceMetadata);
    when(tableMetadata.getPrimaryKey()).thenReturn(ImmutableList.of(idColumn));
  }

  @Test
  public void save() {
    mockTableMetadata();

    final CassandraTable table = new CassandraTable(session, tableMetadata, ConsistencyLevel.QUORUM, null);

    final Event event = EventBuilder
        .withBody("my_body".getBytes(), ImmutableMap.of("id", "1", "text_col", "my_text", "other", "foo"));

    table.save(ImmutableList.of(event));

    verify(session).execute(argThat(new ArgumentMatcher<BatchStatement>() {
      @Override
      public boolean matches(Object o) {
        if (!(o instanceof BatchStatement)) {
          return false;
        }
        final BatchStatement batch = (BatchStatement) o;
        final ConsistencyLevel consistencyLevel = batch.getConsistencyLevel();
        final Statement statement = batch.getStatements().iterator().next();
        log.debug("Batch got consistency: {}", consistencyLevel);
        log.debug("Batch got insert: {}", statement);
        return consistencyLevel == ConsistencyLevel.QUORUM &&
            "INSERT INTO my_keyspace.my_table(text_col,id) VALUES ('my_text',1);".equals(statement.toString());
      }
    }));
    verifyNoMoreInteractions(session);
  }

  @Test
  public void saveUnmappable() {
    mockTableMetadata();

    final CassandraTable table = new CassandraTable(session, tableMetadata, ConsistencyLevel.QUORUM, null);

    final Event event = EventBuilder
        .withBody("my_body".getBytes(), ImmutableMap.of("XXXid", "1", "XXXtext_col", "my_text"));

    table.save(ImmutableList.of(event));
    verifyZeroInteractions(session);
  }

  @Test
  public void saveWithNoPrimaryKey() {
    mockTableMetadata();

    final CassandraTable table = new CassandraTable(session, tableMetadata, ConsistencyLevel.QUORUM, null);

    final Event event = EventBuilder
        .withBody("my_body".getBytes(), ImmutableMap.of("XXXid", "1", "text_col", "my_text"));

    table.save(ImmutableList.of(event));
    verifyZeroInteractions(session);
  }

  @Test
  public void saveEmpty() {
    mockTableMetadata();

    final CassandraTable table = new CassandraTable(session, tableMetadata, ConsistencyLevel.QUORUM, null);
    table.save(ImmutableList.<Event>of());
    verifyZeroInteractions(session);
  }

  @Test
  public void parseWithoutBody() {
    mockTableMetadata();

    final CassandraTable table = new CassandraTable(session, tableMetadata, ConsistencyLevel.QUORUM, null);

    final Event event = EventBuilder
        .withBody("my_body".getBytes(), ImmutableMap.of("id", "1", "text_col", "my_text", "other", "foo"));
    final Map<String, Object> parsedEvent = table.parse(event);
    assertThat(parsedEvent.size()).isEqualTo(2);
    assertThat(parsedEvent.get("id")).isEqualTo(1);
    assertThat(parsedEvent.get("text_col")).isEqualTo("my_text");
  }

  @Test
  public void parseWithBody() {
    mockTableMetadata();

    final CassandraTable table = new CassandraTable(session, tableMetadata, ConsistencyLevel.QUORUM, "text_col");

    final Event event = EventBuilder
        .withBody("my_body".getBytes(), ImmutableMap.of("id", "1", "text_col", "my_text", "other", "foo"));
    final Map<String, Object> parsedEvent = table.parse(event);
    assertThat(parsedEvent.size()).isEqualTo(2);
    assertThat(parsedEvent.get("id")).isEqualTo(1);
    assertThat(parsedEvent.get("text_col")).isEqualTo("my_body");
  }

}
