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
package com.stratio.ingestion.serializer.elasticsearch;

import static org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer.charset;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stratio.ingestion.serializer.elasticsearch.ElasticSearchSerializerWithMapping;

public class TestElasticSearchSerializerWithMapping {
	
	private final static String INDEX_PREFIX = "flume";
	private final static String INDEX_TYPE = "log";
	private final static String MAPPING_PATH = "/mapping.json";
	private final static String TIMESTAMP_HEADER = "timestamp";
	private final static long DAY_IN_MILLIS = 86400000;

	private Node node;
	private ElasticSearchSerializerWithMapping serializer;
	private Client client;
	private String expectedESMapping = "";
	
	@Before
	public void setUpES() throws IOException {
        FileUtils.deleteDirectory(new File("data/test"));
        String jsonMapping = IOUtils.toString(this.getClass()
                .getResourceAsStream(MAPPING_PATH));
		expectedESMapping = Strings.trimAllWhitespace("{\"" + INDEX_TYPE + "\":" + jsonMapping + "}");
		
		URL resourceUrl = getClass().getResource(MAPPING_PATH);
		Context context = new Context();
		context.put("mappingFile", resourceUrl.getPath());
		serializer = new ElasticSearchSerializerWithMapping();
		serializer.configure(context);
		
		ImmutableSettings.Builder ESSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false")
                .put("cluster.name", "test");
		node = nodeBuilder().local(true).settings(ESSettings.build()).node();
		client = node.client();	
	}

    @After
    public void tearDownES() throws IOException {
        client.close();
        node.stop();
        node.close();
        client = null;
        node = null;
    }
	
	@Test
	public void sameTimestampEventsShouldCreateOnlyOneIndexWithTheExpectedMapping() throws IOException {
		Event event = createExampleEvent(System.currentTimeMillis());
		String indexName = getIndexName(INDEX_PREFIX, new TimestampedEvent(event).getTimestamp());
		
		serializer.createIndexRequest(client, INDEX_PREFIX, INDEX_TYPE, event);	
		serializer.createIndexRequest(client, INDEX_PREFIX, INDEX_TYPE, event);
		ImmutableOpenMap<String, MappingMetaData> mappings = getIndices().get(indexName).getMappings();
		String mappingActual = mappings.get(INDEX_TYPE).source().string();
		
		Assert.assertTrue("The index must exists and its mapping must be the same as the expected", mappingActual.equals(expectedESMapping));
	}
	
	@Test
	public void differentTimestampEventsShouldCreateDifferentIndecesWithTheExpectedMapping() throws IOException {
		long timestamp = System.currentTimeMillis();
		Event event1 = createExampleEvent(timestamp);
		String indexName1 = getIndexName(INDEX_PREFIX,  new TimestampedEvent(event1).getTimestamp());
		Event event2 = createExampleEvent(timestamp + DAY_IN_MILLIS);
		String indexName2 = getIndexName(INDEX_PREFIX,  new TimestampedEvent(event2).getTimestamp());
		
		serializer.createIndexRequest(client, INDEX_PREFIX, INDEX_TYPE, event1);	
		serializer.createIndexRequest(client, INDEX_PREFIX, INDEX_TYPE, event2);
		ImmutableOpenMap<String, IndexMetaData> indices = getIndices();
		ImmutableOpenMap<String, MappingMetaData> mappingsIndex1 = indices.get(indexName1).getMappings();
		ImmutableOpenMap<String, MappingMetaData> mappingsIndex2 = indices.get(indexName2).getMappings();
		String mappingIndex1 = mappingsIndex1.get(INDEX_TYPE).source().string();
		String mappingIndex2 = mappingsIndex2.get(INDEX_TYPE).source().string();
		
		Assert.assertTrue("The first index must exists and its mapping must be the same as the expected", mappingIndex1.equals(expectedESMapping));
		Assert.assertTrue("The second index must exists and its mapping must be the same as the expected", mappingIndex2.equals(expectedESMapping));
	}

	private String getIndexName(String indexPrefix, long timestamp) {
		return new StringBuilder(indexPrefix).append('-')
				.append(ElasticSearchIndexRequestBuilderFactory.df
						.format(timestamp)).toString();
	}
	
	private Event createExampleEvent(long timestamp) {
		String message = "test body";
	    Map<String, String> headers = Maps.newHashMap();
	    headers.put(TIMESTAMP_HEADER, String.valueOf(timestamp));
	    Event event = EventBuilder.withBody(message.getBytes(charset));
	    event.setHeaders(headers);
	    return event;
	}
	
	private ImmutableOpenMap<String, IndexMetaData> getIndices() {
		ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().execute().actionGet();
		return clusterStateResponse.getState().getMetaData().getIndices();
	}
	
}
