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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyListOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestCassandraSink {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void confWithMissingTablesFails() {
    final CassandraSink sink = new CassandraSink();
    final Context context = new Context();
    thrown.expect(ConfigurationException.class);
    thrown.expectMessage("tables is mandatory");
    sink.configure(context);
  }

  @Test
  public void confWithBadHostsFails() {
    final CassandraSink sink = new CassandraSink();
    final Context context = new Context();
    context.put("tables", "keyspace.table");
    context.put("hosts", "localhost:9badport");
    thrown.expect(ConfigurationException.class);
    thrown.expectMessage("Could not parse host: localhost:9badport");
    thrown.expectCause(new CauseMatcher(IllegalArgumentException.class));
    sink.configure(context);
  }

  @Test
  public void confMissingCqlFileFails() {
    final CassandraSink sink = new CassandraSink();
    final Context context = new Context();
    context.put("tables", "keyspace.table");
    context.put("cqlFile", "/NOT/FOUND/MY.CQL");
    thrown.expect(ConfigurationException.class);
    thrown.expectMessage("Cannot read CQL file: /NOT/FOUND/MY.CQL");
    thrown.expectCause(new CauseMatcher(FileNotFoundException.class));
    sink.configure(context);
  }

  @Test
  public void processEmtpyChannel() throws EventDeliveryException {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Transaction tx = mock(Transaction.class);
    final CassandraTable table = mock(CassandraTable.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.tables = Collections.singletonList(table);
    sink.setChannel(channel);
    when(channel.getTransaction()).thenReturn(tx);
    when(channel.take()).thenReturn(null);
    assertThat(sink.process()).isEqualTo(Sink.Status.READY);
    verifyZeroInteractions(table);
  }

  @Test
  public void processOneEvent() throws EventDeliveryException {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Transaction tx = mock(Transaction.class);
    final CassandraTable table = mock(CassandraTable.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.tables = Collections.singletonList(table);
    sink.setChannel(channel);
    when(channel.getTransaction()).thenReturn(tx);
    final Event event = EventBuilder.withBody(new byte[0], ImmutableMap.of("id", "1", "col", "text"));
    when(channel.take()).thenReturn(event).thenReturn(null);
    assertThat(sink.process()).isEqualTo(Sink.Status.READY);
    verify(table).save(ImmutableList.of(event));
    verifyNoMoreInteractions(table);
    verify(tx).begin();
    verify(tx).commit();
    verify(tx).close();
  }

  @Test
  public void processOneEventWithBatchSizeOne() throws EventDeliveryException {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Transaction tx = mock(Transaction.class);
    final CassandraTable table = mock(CassandraTable.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    ctx.put("batchSize", "1");
    sink.configure(ctx);
    sink.tables = Collections.singletonList(table);
    sink.setChannel(channel);
    when(channel.getTransaction()).thenReturn(tx);
    final Event event = EventBuilder.withBody(new byte[0], ImmutableMap.of("id", "1", "col", "text"));
    when(channel.take()).thenReturn(event).thenReturn(null);
    assertThat(sink.process()).isEqualTo(Sink.Status.READY);
    verify(table).save(ImmutableList.of(event));
    verifyNoMoreInteractions(table);
    verify(tx).begin();
    verify(tx).commit();
    verify(tx).close();
  }

  @Test
  public void processDriverException() throws EventDeliveryException {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Transaction tx = mock(Transaction.class);
    final CassandraTable table = mock(CassandraTable.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.tables = Collections.singletonList(table);
    sink.setChannel(channel);
    when(channel.getTransaction()).thenReturn(tx);
    final Event event = EventBuilder.withBody(new byte[0], ImmutableMap.of("id", "1", "col", "text"));
    when(channel.take()).thenReturn(event).thenReturn(null);
    doThrow(DriverException.class).when(table).save(anyListOf(Event.class));
    boolean hasThrown = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      hasThrown = true;
      if (!(ex.getCause() instanceof DriverException)) {
        fail("Did not throw inner DriverException: " + ex);
      }
    }
    verify(tx).begin();
    verify(tx).rollback();
    verify(tx).close();
    verifyNoMoreInteractions(tx);
    if (!hasThrown) {
      fail("Did not throw exception");
    }
  }

  @Test
  public void processDriverExceptionWithRollbackException() throws EventDeliveryException {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Transaction tx = mock(Transaction.class);
    final CassandraTable table = mock(CassandraTable.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.tables = Collections.singletonList(table);
    sink.setChannel(channel);
    when(channel.getTransaction()).thenReturn(tx);
    final Event event = EventBuilder.withBody(new byte[0], ImmutableMap.of("id", "1", "col", "text"));
    when(channel.take()).thenReturn(event).thenReturn(null);
    doThrow(DriverException.class).when(table).save(anyListOf(Event.class));
    doThrow(RuntimeException.class).when(tx).rollback();
    boolean hasThrown = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      hasThrown = true;
      if (!(ex.getCause() instanceof DriverException)) {
        fail("Did not throw inner DriverException: " + ex);
      }
    }
    verify(tx).begin();
    verify(tx).rollback();
    verify(tx).close();
    verifyNoMoreInteractions(tx);
    if (!hasThrown) {
      fail("Did not throw exception");
    }
  }

  @Test
  public void processIllegalArgumentException() throws EventDeliveryException {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Transaction tx = mock(Transaction.class);
    final CassandraTable table = mock(CassandraTable.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.tables = Collections.singletonList(table);
    sink.setChannel(channel);
    when(channel.getTransaction()).thenReturn(tx);
    final Event event = EventBuilder.withBody(new byte[0], ImmutableMap.of("id", "1", "col", "text"));
    when(channel.take()).thenReturn(event).thenReturn(null);
    doThrow(IllegalArgumentException.class).when(table).save(anyListOf(Event.class));
    boolean hasThrown = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      hasThrown = true;
      if (!(ex.getCause() instanceof IllegalArgumentException)) {
        fail("Did not throw inner IllegalArgumentException: " + ex);
      }
    }
    verify(tx).begin();
    verify(tx).rollback();
    verify(tx).close();
    verifyNoMoreInteractions(tx);
    if (!hasThrown) {
      fail("Did not throw exception");
    }
  }

  @Test
  public void processRuntimeException() throws EventDeliveryException {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Transaction tx = mock(Transaction.class);
    final CassandraTable table = mock(CassandraTable.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.tables = Collections.singletonList(table);
    sink.setChannel(channel);
    when(channel.getTransaction()).thenReturn(tx);
    final Event event = EventBuilder.withBody(new byte[0], ImmutableMap.of("id", "1", "col", "text"));
    when(channel.take()).thenReturn(event).thenReturn(null);
    doThrow(RuntimeException.class).when(table).save(anyListOf(Event.class));
    boolean hasThrown = false;
    try {
      sink.process();
    } catch (RuntimeException ex) {
      hasThrown = true;
    }
    verify(tx).begin();
    verify(tx).rollback();
    verify(tx).close();
    verifyNoMoreInteractions(tx);
    if (!hasThrown) {
      fail("Did not throw exception");
    }
  }

  @Test
  public void stop() {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Session session = mock(Session.class);
    final Cluster cluster = mock(Cluster.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.setChannel(channel);
    sink.session = session;
    sink.cluster = cluster;
    sink.stop();
    verify(session).isClosed();
    verify(session).close();
    verifyNoMoreInteractions(session);
    verify(cluster).isClosed();
    verify(cluster).close();
    verifyNoMoreInteractions(cluster);
  }

  @Test
  public void stopWithDriverInternalException() {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Session session = mock(Session.class);
    final Cluster cluster = mock(Cluster.class);
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.setChannel(channel);
    sink.session = session;
    sink.cluster = cluster;
    doThrow(DriverInternalError.class).when(session).close();
    doThrow(DriverInternalError.class).when(cluster).close();
    sink.stop();
    verify(session).isClosed();
    verify(session).close();
    verifyNoMoreInteractions(session);
    verify(cluster).isClosed();
    verify(cluster).close();
    verifyNoMoreInteractions(cluster);
  }

  @Test
  public void thatParseWorksOnIgnoreCase() throws EventDeliveryException {
    final CassandraSink sink = new CassandraSink();
    final Channel channel = mock(Channel.class);
    final Transaction tx = mock(Transaction.class);
    final Session session = mock(Session.class);
    final ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;
    String bodyColumn = null;
    TableMetadata tableMetadata = mock(TableMetadata.class);

    // we want to test the method CassandraTable.parse so...we don't mock it
    final CassandraTable tableWithoutIgnoreCase = new CassandraTable(session, tableMetadata, consistencyLevel,
        bodyColumn);

    // create a context without ignore case
    final Context ctx = new Context();
    ctx.put("tables", "keyspace.table");
    sink.configure(ctx);
    sink.tables = Collections.singletonList(tableWithoutIgnoreCase);
    sink.setChannel(channel);
    when(channel.getTransaction()).thenReturn(tx);

    // mock table metadata
    mockTableMetadataWithIdAndNameColumns(tableMetadata);

    // put event names in upper case
    final Event event = EventBuilder.withBody(new byte[0], ImmutableMap.of("ID", "1", "NAME", "text"));

    // parsed result should be empty
    final Map<String, Object> parsedResult = tableWithoutIgnoreCase.parse(event);
    assertThat(parsedResult).isEmpty();

    // now with ignore case --> should be some results
    final CassandraTable tableWitIgnoreCase = new CassandraTable(session, tableMetadata, consistencyLevel,
        bodyColumn, true);
    ctx.put("ignoreCase", "true");
    final Map<String, Object> parsedResultIgnoreCase = tableWitIgnoreCase.parse(event);
    assertThat(parsedResultIgnoreCase).isNotEmpty();
    assertThat(parsedResultIgnoreCase.get("id")).isNotNull();

  }

  private void mockTableMetadataWithIdAndNameColumns(TableMetadata tableMetadata) {
    ColumnMetadata colId = mock(ColumnMetadata.class);
    when(colId.getName()).thenReturn("id");
    when(colId.getType()).thenReturn(DataType.text());
    ColumnMetadata colName = mock(ColumnMetadata.class);
    when(colName.getName()).thenReturn("name");
    when(colName.getType()).thenReturn(DataType.text());

    List<ColumnMetadata> listOfColumns = ImmutableList.of(colId, colName);
    when(tableMetadata.getColumns()).thenReturn(listOfColumns);
  }

  static class CauseMatcher extends BaseMatcher<Throwable> {
    final Class<?> cause;

    public CauseMatcher(final Class<?> cause) {
      this.cause = cause;
    }

    @Override
    public boolean matches(Object item) {
      return item != null && cause.equals(item.getClass());
    }

    @Override
    public void describeTo(Description description) {

    }
  }

}
