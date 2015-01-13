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
        context.put("table", TABLE);
        context.put("port", Integer.toString(contactPoint.getPort()));
        context.put("host", contactPoint.getAddress().getHostAddress());
        context.put("keyspace", KEYSPACE);
        context.put("cluster", "Test Cluster");
        context.put("batchSize", "1");
        URL resourceUrl = getClass().getResource("/definitionAllTypes.json");
        context.put("definitionFile", resourceUrl.getPath());
        context.put("consistency", "QUORUM");

        context.put(
                "keyspaceStatement",
                "CREATE KEYSPACE IF NOT EXISTS keyspaceTest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        context.put("tableStatement",
                "CREATE TABLE if not exists keyspaceTest.tableTest ("
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
	public void textFieldAllowsText() throws EventDeliveryException {
		testFieldType(TEXT_FIELD, "text", Status.READY);
	}

	@Test
	public void intFieldAllowsIntegers() throws EventDeliveryException {
		testFieldType(INT_FIELD, "123", Status.READY);
	}
	
	@Test
	public void intFieldDoesNotAllowText() throws EventDeliveryException {
		testFieldType(INT_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void varcharFieldAllowsText() throws EventDeliveryException {
		testFieldType(VARCHAR_FIELD, "varchar", Status.READY);
	}

	@Test
	public void varintFieldAllowsIntegers() throws EventDeliveryException {
		testFieldType(VARINT_FIELD, "123", Status.READY);
	}

	@Test
	public void varintFieldDoesNotAllowText() throws EventDeliveryException {
		testFieldType(VARINT_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void asciiFieldAllowsText() throws EventDeliveryException {
		testFieldType(ASCII_FIELD, "abcd", Status.READY);
	}

	@Test
	public void booleanFieldAllowsAnything() throws EventDeliveryException {
		testFieldType(BOOLEAN_FIELD, "false", Status.READY);
	}

	@Test
	public void decimalFieldAllowsFloats() throws EventDeliveryException {
		testFieldType(DECIMAL_FIELD, "123.45", Status.READY);
	}

	@Test
	public void decimalFieldAllowsIntegers() throws EventDeliveryException {
		testFieldType(DECIMAL_FIELD, "123", Status.READY);
	}

	@Test
	public void decimalFieldDoesNotAllowText() throws EventDeliveryException {
		testFieldType(DECIMAL_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void doubleFieldAllowsIntegers() throws EventDeliveryException {
		testFieldType(DOUBLE_FIELD, "123", Status.READY);
	}

	@Test
	public void doubleFieldDoesNotAllowText() throws EventDeliveryException {
		testFieldType(DOUBLE_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void floatFieldAllowsFloats() throws EventDeliveryException {
		testFieldType(FLOAT_FIELD, "123.45", Status.READY);
	}

	@Test
	public void floatFieldAllowsIntegers() throws EventDeliveryException {
		testFieldType(FLOAT_FIELD, "123", Status.READY);
	}

	@Test
	public void floatFieldDoesNotAllowText() throws EventDeliveryException {
		testFieldType(FLOAT_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void inetFieldAllowsInet() throws EventDeliveryException {
		testFieldType(INET_FIELD, "123.10.123.10", Status.READY);
	}

	@Test
	public void inetFieldDoesNotAllowText() throws EventDeliveryException {
		testFieldType(INET_FIELD, "text", Status.BACKOFF);
	}

	@Test
	public void listFieldAllowsList() throws EventDeliveryException {
		testFieldType(LIST_FIELD, "a,b,c,d,e", Status.READY);
	}

	@Test
	public void mapFieldAllowsMap() throws EventDeliveryException {
		testFieldType(MAP_FIELD, "a,0;c,1", Status.READY);
	}

	@Test
	public void setFieldAllowsList() throws EventDeliveryException {
		testFieldType(SET_FIELD, "a,b,c,d,e", Status.READY);
	}

	@Test
	public void timestampFieldAllowsDatesWithTheFormatDefined()
			throws EventDeliveryException {
		testFieldType(TIMESTAMP_FIELD, "10/10/2010", Status.READY);
	}

	@Test
	public void timestampFieldDoesNotAllowDatesWithOtherFormatThatTheDefined()
			throws EventDeliveryException {
		testFieldType(TIMESTAMP_FIELD, "2010", Status.BACKOFF);
	}

	@Test
	public void UUIDFieldAllowsUUID() throws EventDeliveryException {
		testFieldType(UUID_FIELD, "550e8400-e29b-41d4-a716-446655440000", Status.READY);
	}

	@Test
	public void UUIDFieldDoesNotAllowInvalidUUID() throws EventDeliveryException {
		testFieldType(UUID_FIELD, "550e8400", Status.BACKOFF);
	}

	@Test
	public void bigintFieldAllowsIntegers() throws EventDeliveryException {
		testFieldType(BIGINT_FIELD, "12345", Status.READY);
	}

	@Test
	public void bigintFieldDoesNotAllowText() throws EventDeliveryException {
		testFieldType(BIGINT_FIELD, "text", Status.BACKOFF);
	}

	private void testFieldType(final String field, final String value, final Status result) throws EventDeliveryException {
		headers.put(field, value);
		addEventToChannel(headers);
		Status status = sink.process();
		Assert.assertEquals(result, status);
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