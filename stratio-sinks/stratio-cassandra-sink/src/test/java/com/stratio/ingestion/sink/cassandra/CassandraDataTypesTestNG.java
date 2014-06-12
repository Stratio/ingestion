package com.stratio.ingestion.sink.cassandra;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;

public class CassandraDataTypesTestNG {

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

	private Context context;

	private MemoryChannel channel;
	private CassandraSink sink;

	private Map<String, String> headers;

	@BeforeClass
	public void beforeClass() throws ConfigurationException, TTransportException, IOException, InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		context = new Context();
		context.put("table", TABLE);
		context.put("port", "9142");
		context.put("host", "localhost");
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
	}

	@BeforeMethod
	public void before() throws TTransportException, IOException,
			InterruptedException, ConfigurationException {
		Context channelContext = new Context();
		channelContext.put("capacity", "10000");
		channelContext.put("transactionCapacity", "200");
		channel = new MemoryChannel();
		channel.setName("junitChannel");
		Configurables.configure(channel, channelContext);
		sink.setChannel(channel);
		
		sink.start();
		headers = new HashMap<>();
		headers.put(PRIMARY_KEY, UUID.randomUUID().toString());
	}

	@Test
	public void textFieldAllowsText() throws EventDeliveryException {
		headers.put(TEXT_FIELD, "text");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void intFieldAllowsIntegers() throws EventDeliveryException {
		headers.put(INT_FIELD, "123");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}
	
	@Test
	public void intFieldDoesNotAllowText() throws EventDeliveryException {
		headers.put(INT_FIELD, "text");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@Test
	public void varcharFieldAllowsText() throws EventDeliveryException {
		headers.put(VARCHAR_FIELD, "varchar");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void varintFieldAllowsIntegers() throws EventDeliveryException {
		headers.put(VARINT_FIELD, "123");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void varintFieldDoesNotAllowText() throws EventDeliveryException {
		headers.put(VARINT_FIELD, "text");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@Test
	public void asciiFieldAllowsText() throws EventDeliveryException {
		headers.put(ASCII_FIELD, "abcd");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void booleanFieldAllowsAnything() throws EventDeliveryException {
		headers.put(BOOLEAN_FIELD, "false");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void decimalFieldAllowsFloats() throws EventDeliveryException {
		headers.put(DECIMAL_FIELD, "123.45");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void decimalFieldAllowsIntegers() throws EventDeliveryException {
		headers.put(DECIMAL_FIELD, "123");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void decimalFieldDoesNotAllowText() throws EventDeliveryException {
		headers.put(DECIMAL_FIELD, "text");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@Test
	public void doubleFieldAllowsIntegers() throws EventDeliveryException {
		headers.put(DOUBLE_FIELD, "123");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void doubleFieldDoesNotAllowText() throws EventDeliveryException {
		headers.put(DOUBLE_FIELD, "text");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@Test
	public void floatFieldAllowsFloats() throws EventDeliveryException {
		headers.put(FLOAT_FIELD, "123.45");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void floatFieldAllowsIntegers() throws EventDeliveryException {
		headers.put(FLOAT_FIELD, "123");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void floatFieldDoesNotAllowText() throws EventDeliveryException {
		headers.put(FLOAT_FIELD, "text");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@Test
	public void inetFieldAllowsInet() throws EventDeliveryException {
		headers.put(INET_FIELD, "123.10.123.10");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void inetFieldDoesNotAllowText() throws EventDeliveryException {
		headers.put(INET_FIELD, "text");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@Test
	public void listFieldAllowsList() throws EventDeliveryException {
		headers.put(LIST_FIELD, "a,b,c,d,e");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void mapFieldAllowsMap() throws EventDeliveryException {
		headers.put(MAP_FIELD, "a,b;c,d");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void setFieldAllowsList() throws EventDeliveryException {
		headers.put(SET_FIELD, "a,b,c,d,e");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void timestampFieldAllowsDatesWithTheFormatDefined()
			throws EventDeliveryException {
		headers.put(TIMESTAMP_FIELD, "10/10/2010");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void timestampFieldDoesNotAllowDatesWithOtherFormatThatTheDefined()
			throws EventDeliveryException {
		headers.put(TIMESTAMP_FIELD, "2010");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@Test
	public void UUIDFieldAllowsUUID() throws EventDeliveryException {
		headers.put(UUID_FIELD, "550e8400-e29b-41d4-a716-446655440000");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void UUIDFieldDoesNotAllowInvalidUUID()
			throws EventDeliveryException {
		headers.put(UUID_FIELD, "550e8400");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@Test
	public void bigintFieldAllowsIntegers() throws EventDeliveryException {
		headers.put(BIGINT_FIELD, "12345");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void bigintFieldDoesNotAllowText() throws EventDeliveryException {
		headers.put(BIGINT_FIELD, "text");
		addEventToChannel(headers);

		Status status = sink.process();
		Assert.assertEquals(status, Status.BACKOFF);
	}

	@AfterMethod
	public void tearDown() {
		sink.stop();
	}

	@AfterClass
	public void afterClass() throws InterruptedException {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
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