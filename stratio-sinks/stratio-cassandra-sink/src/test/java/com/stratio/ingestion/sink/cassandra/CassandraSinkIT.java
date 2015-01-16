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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;

@RunWith(JUnit4.class)
public class CassandraSinkIT {

	private MemoryChannel channel;
	private CassandraSink sink;

	private Map<String, String> headers;

	private void _do() throws TTransportException, IOException, InterruptedException {
        final Context context = new Context();
				final InetSocketAddress contactPoint = CassandraTestHelper.getCassandraContactPoint();
        context.put("table", "keyspaceTestCassandraSinkIT.tableTestCassandraSinkIT");
        context.put("port", Integer.toString(contactPoint.getPort()));
        context.put("host", contactPoint.getAddress().getHostAddress());
        context.put("cluster", "Test Cluster");
        context.put("batchSize", "1");
        context.put("consistency", "QUORUM");

				final File cqlFile = File.createTempFile("flumeTest", "cql");
				cqlFile.deleteOnExit();

				IOUtils.write(
						"CREATE KEYSPACE IF NOT EXISTS keyspaceTestCassandraSinkIT WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };\n"
						+ "CREATE TABLE IF NOT EXISTS keyspaceTestCassandraSinkIT.tableTestCassandraSinkIT ("
						+ "id uuid, bool_field boolean, int_field int, PRIMARY KEY (int_field)"
						+ ")",
						new FileOutputStream(cqlFile));

				context.put("cqlFile", cqlFile.getAbsolutePath());
        sink = new CassandraSink();
        sink.configure(context);

		Context channelContext = new Context();
		channelContext.put("capacity", "10000");
		channelContext.put("transactionCapacity", "200");
		channel = new MemoryChannel();
		channel.setName("junitChannel");
		Configurables.configure(channel, channelContext);
		sink.setChannel(channel);
		
		sink.start();
		sink.stop();
	}

	@Test
	public void initializeCqlTwice() throws TTransportException, IOException, InterruptedException {
		final InetSocketAddress contactPoint = CassandraTestHelper.getCassandraContactPoint();
		Cluster cluster = Cluster.builder()
				.addContactPointsWithPorts(Collections.singletonList(contactPoint))
				.build();
		Session session = cluster.connect();

		session.execute("DROP KEYSPACE IF EXISTS keyspaceTestCassandraSinkIT");
		Assert.assertNull(session.getCluster().getMetadata().getKeyspace("keyspaceTestCassandraSinkIT"));
		_do();
		Assert.assertNotNull(session.getCluster().getMetadata().getKeyspace("keyspaceTestCassandraSinkIT"));
		Assert.assertNotNull(session.getCluster().getMetadata().getKeyspace("keyspaceTestCassandraSinkIT")
				.getTable("tableTestCassandraSinkIT"));
		_do();
		Assert.assertNotNull(session.getCluster().getMetadata().getKeyspace("keyspaceTestCassandraSinkIT"));
		Assert.assertNotNull(session.getCluster().getMetadata().getKeyspace("keyspaceTestCassandraSinkIT")
				.getTable("tableTestCassandraSinkIT"));
		session.execute("DROP KEYSPACE IF EXISTS keyspaceTestCassandraSinkIT");

		session.close();
		cluster.close();
	}

}