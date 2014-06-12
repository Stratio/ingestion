package com.stratio.ingestion.sink.cassandra;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class EventParserJUnit4Test {

	private EventParser instance;

	@Before
	public void before() throws IOException {
		this.instance = new EventParser(IOUtils.toString(this.getClass()
				.getResourceAsStream("/definition.json")));
	}

	@Test
	public void shouldParseDate() throws Exception {
		Date date = EventParser.parseDate("01/02/2014 12:00:00",
				"dd/MM/yyyy HH:mm:ss");
		Assert.assertEquals(date, getDate());
	}

	@Test
	public void shoudParsePrimitiveTypes() throws Exception {
		Integer integer = (Integer) EventParser.parseValue("1",
				CassandraDataType.valueOf("INT"), "");
		Assert.assertEquals(new Integer(1), integer);

		String string = (String) EventParser.parseValue("string",
				CassandraDataType.valueOf("TEXT"), "");
		Assert.assertEquals("string", string);

		Boolean bool = (Boolean) EventParser.parseValue("true",
				CassandraDataType.valueOf("BOOLEAN"), "");
		Assert.assertEquals(new Boolean(true), bool);

		InetAddress addr = (InetAddress) EventParser.parseValue("192.168.1.1",
				CassandraDataType.valueOf("INET"), "");
		Assert.assertEquals(InetAddress.getByName("192.168.1.1"), addr);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void shouldParseLists() throws Exception {
		String rawList = "1;2;3";
		CassandraField<List> listField = EventParser.parseList(rawList,
				this.instance.getDefinition().getFields().get(1));
		Assert.assertEquals(listField.getValue().get(0), new Integer(1));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void shouldParseMap() throws Exception {
		String rawMap = "1:192.168.1.1;2:192.168.1.2;3:192.168.1.3";
		CassandraField<Map> mapField = EventParser.parseMap(rawMap,
				this.instance.getDefinition().getFields().get(2));
		Map<Integer, InetAddress> expected = new HashMap<Integer, InetAddress>();
		expected.put(1, InetAddress.getByName("192.168.1.1"));
		Assert.assertEquals(expected.get(1), mapField.getValue().get(1));
	}

	private final static Date getDate() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, 2014);
		calendar.set(Calendar.MONTH, 1);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		calendar.set(Calendar.HOUR_OF_DAY, 12);
		calendar.set(Calendar.MINUTE, 00);
		calendar.set(Calendar.SECOND, 00);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTime();
	}
}
