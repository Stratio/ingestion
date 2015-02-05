package com.stratio.ingestion.sink.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

final class CassandraUtils {

  private CassandraUtils() {

  }

  public static void executeCqlScript(final Session session, final String script) {
    if (script == null) {
      return;
    }
    final List<String> lines = new ArrayList<String>();
    for (final String line : Splitter.on("\n").split(script)) {
      lines.add(line.trim());
    }
    for (String cql : Joiner.on(" ").join(lines).split(";")) {
      cql = cql.trim();
      if (cql.isEmpty()) {
        continue;
      }
      session.execute(cql);
    }
  }

  public static TableMetadata getTableMetadata(final Session session, final String keyspace, final String table) {
    Preconditions.checkNotNull(session);
    Preconditions.checkNotNull(keyspace);
    Preconditions.checkNotNull(table);
    final KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(keyspace);
    if (keyspaceMetadata == null) {
      throw new IllegalStateException(String.format("Keyspace %s does not exist", keyspace));
    }
    final TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
    if (tableMetadata == null) {
      throw new IllegalStateException(String.format("Table %s.%s does not exist", keyspace, table));
    }
    return tableMetadata;
  }

  public static Object parseValue(final DataType type, final String value) {
    if (value == null) {
      return null;
    }
    switch (type.getName()) {
    case TEXT:
    case VARCHAR:
    case ASCII:
      return value;
    case INET:
      return type.parse("'" + value + "'");
    case INT:
    case VARINT:
    case BIGINT:
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
    case BOOLEAN:
      return type.parse(value.replaceAll("\\s+", ""));
    default:
      return type.parse(value);
    }
  }

}
