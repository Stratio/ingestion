package com.stratio.ingestion.sink.cassandra;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class FieldTypeToCassandraTypeJUnit4Test {

	private FieldDefinition definition;
	
	@Before
	public void setUp() {
		definition = new FieldDefinition();
	}
	
	@Test
	public void textTest() {
		definition.setType("TEXT");
		
		String cassandraType = definition.getCassandraType();
		
		Assert.assertEquals("TEXT", cassandraType);
	}
	
	@Test
	public void listTest() {
		definition.setType("LIST");
		definition.setItemSeparator(",");
		definition.setListValueType("TEXT");
		
		String cassandraType = definition.getCassandraType();
		
		Assert.assertEquals("LIST<TEXT>", cassandraType);
	}
	
	@Test
	public void setTest() {
		definition.setType("SET");
		definition.setItemSeparator(",");
		definition.setListValueType("TEXT");
		
		String cassandraType = definition.getCassandraType();
		
		Assert.assertEquals("SET<TEXT>", cassandraType);
	}
	
	@Test
	public void mapTest() {
		definition.setType("MAP");
		definition.setItemSeparator(";");
		definition.setMapKeyType("TEXT");
		definition.setMapValueSeparator(",");
		definition.setMapValueType("INT");
		
		String cassandraType = definition.getCassandraType();
		
		Assert.assertEquals("MAP<TEXT,INT>", cassandraType);
	}
	
}