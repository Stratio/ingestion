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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
public class CassandraDataTypesIT {

    private static final Logger log = LoggerFactory.getLogger(CassandraDataTypesIT.class);

	private final static String KEYSPACE = "keyspaceTest";
	private final static String TABLE = "tableTest";
	private final static String PRIMARY_KEY = "id_field";

	private final static String TEXT_FIELD = "text_field";
	private final static String VARCHAR_FIELD = "varchar_field";
	private final static String VARINT_FIELD = "varint_field";
	private final static String ASCII_FIELD = "ascii_field";
	private final static String BOOLEAN_FIELD = "boolean_field";
	private final static String DECIMAL_FIELD = "decimal_field";
	private final static String DOUBLE_FIELD = "double_field";
	private final static String FLOAT_FIELD = "float_field";
	private final static String INET_FIELD = "inet_field";
	private final static String INT_FIELD = "int_field";
	private final static String LIST_FIELD = "list_field";
	private final static String MAP_FIELD = "map_field";
	private final static String SET_FIELD = "set_field";
	private final static String TIMESTAMP_FIELD = "timestamp_field";
	private final static String UUID_FIELD = "uuid_field";
	private final static String BIGINT_FIELD = "bigint_field";

	private MemoryChannel channel;
	private CassandraSink sink;

	private Map<String, String> headers;

	@Before
	public void setup() throws TTransportException, IOException,
			InterruptedException {
        final Context context = new Context();
				final InetSocketAddress contactPoint = CassandraTestHelper.getCassandraContactPoint();
        context.put("table", KEYSPACE + "." + TABLE);
        context.put("port", Integer.toString(contactPoint.getPort()));
        context.put("host", contactPoint.getAddress().getHostAddress());
        context.put("cluster", "Test Cluster");
        context.put("batchSize", "1");
        context.put("consistency", "QUORUM");

				Cluster cluster = Cluster.builder()
				.addContactPointsWithPorts(Collections.singletonList(contactPoint))
				.build();
				Session session = cluster.connect();
				session.execute("CREATE KEYSPACE IF NOT EXISTS keyspaceTest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
				session.execute("CREATE TABLE if not exists keyspaceTest.tableTest ("
						+ PRIMARY_KEY + " uuid, " + TEXT_FIELD + " text, "
						+ VARCHAR_FIELD + " varchar, " + VARINT_FIELD
						+ " varint, " + ASCII_FIELD + " ascii, "
						+ BOOLEAN_FIELD + " boolean, " + DECIMAL_FIELD
						+ " decimal, " + DOUBLE_FIELD + " double, "
						+ FLOAT_FIELD + " float, " + INET_FIELD + " inet, "
						+ INT_FIELD + " int, " + LIST_FIELD + " list<TEXT>, "
						+ MAP_FIELD + " map<TEXT,TEXT>, " + SET_FIELD
						+ " set<TEXT>, " + TIMESTAMP_FIELD + " timestamp, "
						+ UUID_FIELD + " uuid, " + BIGINT_FIELD
						+ " bigint, PRIMARY KEY (" + PRIMARY_KEY + "));");
			session.close();
		cluster.close();

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
		headers = new HashMap<String, String>();
		headers.put(PRIMARY_KEY, UUID.randomUUID().toString());
	}


	@After
	public void tearDown() {
		sink.stop();
	}

	@Test
	public void textFieldAllowsText() {
		testFieldType(TEXT_FIELD, "text", Status.READY);
	}

	@Test
	public void intFieldAllowsIntegers() {
		testFieldType(INT_FIELD, "123", Status.READY);
	}
	
	@Test
	public void intFieldDoesNotAllowText() {
		testFieldType(INT_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void varcharFieldAllowsText() {
		testFieldType(VARCHAR_FIELD, "varchar", Status.READY);
	}

	@Test
	public void varintFieldAllowsIntegers() {
		testFieldType(VARINT_FIELD, "123", Status.READY);
	}

	@Test
	public void varintFieldDoesNotAllowText() {
		testFieldType(VARINT_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void asciiFieldAllowsText() {
		testFieldType(ASCII_FIELD, "abcd", Status.READY);
	}

	@Test
	public void booleanFieldAllowsAnything() {
		testFieldType(BOOLEAN_FIELD, "false", Status.READY);
	}

	@Test
	public void decimalFieldAllowsFloats() {
		testFieldType(DECIMAL_FIELD, "123.45", Status.READY);
	}

	@Test
	public void decimalFieldAllowsIntegers() {
		testFieldType(DECIMAL_FIELD, "123", Status.READY);
	}

	@Test
	public void decimalFieldDoesNotAllowText() {
		testFieldType(DECIMAL_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void doubleFieldAllowsIntegers() {
		testFieldType(DOUBLE_FIELD, "123", Status.READY);
	}

	@Test
	public void doubleFieldDoesNotAllowText() {
		testFieldType(DOUBLE_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void floatFieldAllowsFloats() {
		testFieldType(FLOAT_FIELD, "123.45", Status.READY);
	}

	@Test
	public void floatFieldAllowsIntegers() {
		testFieldType(FLOAT_FIELD, "123", Status.READY);
	}

	@Test
	public void floatFieldDoesNotAllowText() {
		testFieldType(FLOAT_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void inetFieldAllowsInet() {
		testFieldType(INET_FIELD, "123.10.123.10", Status.READY);
	}

	@Test
	public void inetFieldDoesNotAllowText() {
		testFieldType(INET_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void listFieldAllowsList() {
		testFieldType(LIST_FIELD, "a,b,c,d,e", Status.READY);
	}

	@Test
	public void mapFieldAllowsMap() {
		testFieldType(MAP_FIELD, "a:0,c:1", Status.READY);
	}

	@Test
	public void setFieldAllowsList() {
		testFieldType(SET_FIELD, "a,b,c,d,e", Status.READY);
	}

	@Test
	public void timestampFieldAllowsDatesWithTheFormatDefined() {
		testFieldType(TIMESTAMP_FIELD, "1231234", Status.READY);
		testFieldType(TIMESTAMP_FIELD, "2010-12-20T10:20:20", Status.READY);
		testFieldType(TIMESTAMP_FIELD, "2010-12-20T10:20:20.000", Status.READY);
		testFieldType(TIMESTAMP_FIELD, "2010-12-20T10:20:20.000Z", Status.READY);
	}

	@Test
	public void timestampFieldDoesNotAllowDatesWithOtherFormatThatTheDefined() {
		testFieldType(TIMESTAMP_FIELD, "1/2/3/4/5", Status.BACKOFF);
	}

	@Test
	public void UUIDFieldAllowsUUID() {
		testFieldType(UUID_FIELD, "550e8400-e29b-41d4-a716-446655440000", Status.READY);
	}

	@Test
	public void UUIDFieldDoesNotAllowInvalidUUID() {
		testFieldType(UUID_FIELD, "550e8400", Status.BACKOFF);
	}

	@Test
	public void bigintFieldAllowsIntegers() {
		testFieldType(BIGINT_FIELD, "12345", Status.READY);
	}

	@Test
	public void bigintFieldDoesNotAllowText() {
		testFieldType(BIGINT_FIELD, "text", Status.BACKOFF);
	}

	private void testFieldType(final String field, final String value, final Status result) {
		headers.put(field, value);
		addEventToChannel(headers);
		boolean thrown = false;
		try {
			Status status = sink.process();
			Assert.assertEquals(result, status);
		} catch (EventDeliveryException ex) {
			thrown = true;
		}
		if (result == Status.READY) {
			Assert.assertFalse(thrown);
		} else {
			Assert.assertTrue(thrown);
		}
	}

	private void addEventToChannel(Map<String, String> headers) {
		Event event = EventBuilder.withBody("body", Charsets.UTF_8, headers);
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		channel.put(event);
		transaction.commit();
		transaction.close();
	}

}