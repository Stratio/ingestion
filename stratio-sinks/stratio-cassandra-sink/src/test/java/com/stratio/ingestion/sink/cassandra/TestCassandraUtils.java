package com.stratio.ingestion.sink.cassandra;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.collect.ImmutableSet;

public class TestCassandraUtils {

  final Set<DataType> textualTypes = ImmutableSet.of(
      DataType.ascii(),
      DataType.text(),
      DataType.varchar()
  );

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void executeCqlScriptNull() {
    final Session session = mock(Session.class);
    CassandraUtils.executeCqlScript(session, null);
    verifyZeroInteractions(session);
  }

  @Test
  public void executeCqlScriptEmpty() {
    final Session session = mock(Session.class);
    CassandraUtils.executeCqlScript(session, "");
    verifyZeroInteractions(session);
  }

  @Test
  public void executeCqlScriptWhitespaceOnly() {
    final Session session = mock(Session.class);
    CassandraUtils.executeCqlScript(session, "   ");
    verifyZeroInteractions(session);
  }

  @Test
  public void executeCqlScriptOneStatement() {
    final Session session = mock(Session.class);
    CassandraUtils.executeCqlScript(session, " MY STATEMENT;");
    verify(session).execute("MY STATEMENT");
    verifyNoMoreInteractions(session);
  }

  @Test
  public void executeCqlScriptTwoStatements() {
    final Session session = mock(Session.class);
    CassandraUtils.executeCqlScript(session, "MY STATEMENT;\n\nOTHER STATEMENT;");
    verify(session).execute("MY STATEMENT");
    verify(session).execute("OTHER STATEMENT");
    verifyNoMoreInteractions(session);
  }

  @Test
  public void executeCqlScriptTwoStatementsNoFinalSemicolon() {
    final Session session = mock(Session.class);
    CassandraUtils.executeCqlScript(session, "MY STATEMENT;\n\nOTHER STATEMENT");
    verify(session).execute("MY STATEMENT");
    verify(session).execute("OTHER STATEMENT");
    verifyNoMoreInteractions(session);
  }

  @Test(expected = NullPointerException.class)
  public void getTableMetadataOnNullSession() {
    CassandraUtils.getTableMetadata(null, "keyspace", "table");
  }

  @Test(expected = NullPointerException.class)
  public void getTableMetadataOnNullKeyspace() {
    CassandraUtils.getTableMetadata(mock(Session.class), null, "table");
  }

  @Test(expected = NullPointerException.class)
  public void getTableMetadataOnNullTable() {
    CassandraUtils.getTableMetadata(mock(Session.class), "keyspace", null);
  }

  @Test
  public void getTableMetadataOnUnexistentKeyspace() {
    // session.getCluster().getMetadata().getKeyspace(keyspace);
    final Session session = mock(Session.class);
    final Cluster cluster = mock(Cluster.class);
    final Metadata metadata = mock(Metadata.class);
    when(session.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace("keyspace")).thenReturn(null);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Keyspace keyspace does not exist");
    CassandraUtils.getTableMetadata(session, "keyspace", "table");
  }

  @Test
  public void getTableMetadataOnUnexistentTable() {
    final Session session = mock(Session.class);
    final Cluster cluster = mock(Cluster.class);
    final Metadata metadata = mock(Metadata.class);
    final KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
    when(session.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace("keyspace")).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTable("table")).thenReturn(null);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Table keyspace.table does not exist");
    CassandraUtils.getTableMetadata(session, "keyspace", "table");
  }

  @Test
  public void getTableMetadata() {
    final Session session = mock(Session.class);
    final Cluster cluster = mock(Cluster.class);
    final Metadata metadata = mock(Metadata.class);
    final KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
    final TableMetadata tableMetadata = mock(TableMetadata.class);
    when(session.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace("keyspace")).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTable("table")).thenReturn(tableMetadata);
    assertThat(CassandraUtils.getTableMetadata(session, "keyspace", "table")).isNotNull();
  }

  @Test
  public void parseValueNull() {
    for (final DataType dataType : DataType.allPrimitiveTypes()) {
      assertThat(CassandraUtils.parseValue(dataType, null)).isNull();
    }
  }

  @Test
  public void parseValueText() {
    for (final DataType dataType : textualTypes) {
      assertThat(CassandraUtils.parseValue(dataType, "unquoted_text"))
          .isEqualTo("unquoted_text");
      assertThat(CassandraUtils.parseValue(dataType, "'quoted_text'"))
          .isEqualTo("'quoted_text'");
      assertThat(CassandraUtils.parseValue(dataType, "'unescaped_'quoted_text'"))
          .isEqualTo("'unescaped_'quoted_text'");
    }
  }

  @Test
  public void parseInet() throws UnknownHostException {
    assertThat(CassandraUtils.parseValue(DataType.inet(), "127.0.0.1"))
        .isEqualTo(InetAddress.getByName("127.0.0.1"));
  }

  @Test
  public void parseSpacedInt() throws UnknownHostException {
    assertThat(CassandraUtils.parseValue(DataType.cint(), " 1   2   "))
        .isEqualTo(12);
  }

  @Test
  public void parseQuotedInetFails() throws UnknownHostException {
    thrown.expect(InvalidTypeException.class);
    thrown.expectMessage("Cannot parse inet value");
    CassandraUtils.parseValue(DataType.inet(), "'127.0.0.1'");
  }

  @Test
  public void parseBlob() {
    final ByteBuffer buffer = ByteBuffer.allocate(2);
    buffer.put(new byte[] { (byte) 0xF1, 0x56 });
    buffer.flip();
    assertThat(CassandraUtils.parseValue(DataType.blob(), "0xF156"))
        .isEqualTo(buffer);
  }

}
