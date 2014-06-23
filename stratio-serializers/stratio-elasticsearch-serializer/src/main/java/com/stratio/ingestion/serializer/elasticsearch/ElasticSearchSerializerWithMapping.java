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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.elasticsearch.ContentBuilderUtil;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

public class ElasticSearchSerializerWithMapping implements
		ElasticSearchIndexRequestBuilderFactory {
	
	private static final Logger logger = LoggerFactory
		      .getLogger(ElasticSearchSerializerWithMapping.class);
	
	private static final String CONF_MAPPING_FILE = "mappingFile";

	private String jsonMapping = "";
	private String oldIndexName = "";

	@Override
	public void configure(Context context) {
		String mappingFile = context.getString(CONF_MAPPING_FILE);
		try {
			jsonMapping = readFile(new File(mappingFile));
		} catch (IOException e) {
			Throwables.propagate(e);
		}
	}

	/**
	 * Gets the content of the mapping file
	 * 
	 * @return The content of the file in UTF-8
	 * @param file
	 *            File with the mapping
	 */
	private static final String readFile(File file) throws IOException {
		FileInputStream inputStream;
		inputStream = new FileInputStream(file);
		return IOUtils.toString(inputStream, "UTF-8");
	}

	@Override
	public void configure(ComponentConfiguration conf) {}

	@Override
	public IndexRequestBuilder createIndexRequest(Client client,
			String indexPrefix, String indexType, Event event)
			throws IOException {
		IndexRequestBuilder request = prepareIndex(client);
		TimestampedEvent timestampedEvent = new TimestampedEvent(event);
		long timestamp = timestampedEvent.getTimestamp();
		String indexName = getIndexName(indexPrefix, timestamp);
		
		if (!jsonMapping.isEmpty() && !oldIndexName.equals(indexName)) {
			oldIndexName = indexName;
			createIndexWithMapping(client, indexName, indexType);
		}
		
		prepareIndexRequest(request, indexName, indexType, timestampedEvent);
		return request;
	}

	/**
	 * Creates the index if no exists with the mapping defined by the user
	 * 
	 * @param client
	 *       	  ElasticSearch {@link Client}
	 * @param indexName
	 *            Index name to use -- as per
	 *            {@link #getIndexName(String, long)}
	 * @param indexType
	 *            Index type to use -- as configured on the sink
	 */
	private void createIndexWithMapping(Client client, String indexName, String indexType) {
		try {
			client.admin().indices().create(new CreateIndexRequest(indexName).mapping(indexType, jsonMapping)).actionGet();
		} catch (IndexAlreadyExistsException e) {
			logger.info("The index " + indexName + " already exists");
		}
	}

	@VisibleForTesting
	IndexRequestBuilder prepareIndex(Client client) {
		return client.prepareIndex();
	}

	/**
	 * Gets the name of the index to use for an index request
	 * 
	 * @return index name of the form 'indexPrefix-formattedTimestamp'
	 * @param indexPrefix
	 *            Prefix of index name to use -- as configured on the sink
	 * @param timestamp
	 *            timestamp (millis) to format / use
	 */
	private String getIndexName(String indexPrefix, long timestamp) {
		return new StringBuilder(indexPrefix)
				.append('-')
				.append(ElasticSearchIndexRequestBuilderFactory.df
						.format(timestamp)).toString();
	}

	/**
	 * Prepares an ElasticSearch {@link IndexRequestBuilder} instance
	 * 
	 * @param indexRequest
	 *            The (empty) ElasticSearch {@link IndexRequestBuilder} to
	 *            prepare
	 * @param indexName
	 *            Index name to use -- as per
	 *            {@link #getIndexName(String, long)}
	 * @param indexType
	 *            Index type to use -- as configured on the sink
	 * @param event
	 *            Flume event to serialize and add to index request
	 * @throws IOException
	 *             If an error occurs e.g. during serialization
	 */
	private void prepareIndexRequest(IndexRequestBuilder indexRequest,
			String indexName, String indexType, Event event) throws IOException {
		BytesStream contentBuilder = getContentBuilder(event);
		indexRequest.setIndex(indexName).setType(indexType)
				.setSource(contentBuilder.bytes());
	}

	private BytesStream getContentBuilder(Event event) throws IOException {
		XContentBuilder builder = jsonBuilder().startObject();
		appendBody(builder, event);
		appendHeaders(builder, event);
		return builder;
	}

	private void appendBody(XContentBuilder builder, Event event)
			throws IOException, UnsupportedEncodingException {
		byte[] body = event.getBody();
		ContentBuilderUtil.appendField(builder, "@message", body);
	}

	private void appendHeaders(XContentBuilder builder, Event event)
			throws IOException {
		Map<String, String> headers = Maps.newHashMap(event.getHeaders());

		String timestamp = headers.get("timestamp");
		if (!StringUtils.isBlank(timestamp)
				&& StringUtils.isBlank(headers.get("@timestamp"))) {
			long timestampMs = Long.parseLong(timestamp);
			builder.field("@timestamp", new Date(timestampMs));
		}

		for (String key : headers.keySet()) {
			byte[] val = headers.get(key).getBytes(Charset.defaultCharset());
			ContentBuilderUtil.appendField(builder, key, val);
		}
	}

}

/**
 * {@link Event} implementation that has a timestamp. The timestamp is taken
 * from (in order of precedence):
 * <ol>
 * <li>The "timestamp" header of the base event, if present</li>
 * <li>The "@timestamp" header of the base event, if present</li>
 * <li>The current time in millis, otherwise</li>
 * </ol>
 */
final class TimestampedEvent extends SimpleEvent {

	private final long timestamp;

	TimestampedEvent(Event base) {
		setBody(base.getBody());
		Map<String, String> headers = Maps.newHashMap(base.getHeaders());
		String timestampString = headers.get("timestamp");
		if (StringUtils.isBlank(timestampString)) {
			timestampString = headers.get("@timestamp");
		}
		if (StringUtils.isBlank(timestampString)) {
			this.timestamp = DateTimeUtils.currentTimeMillis();
			headers.put("timestamp", String.valueOf(timestamp));
		} else {
			this.timestamp = Long.valueOf(timestampString);
		}
		setHeaders(headers);
	}

	long getTimestamp() {
		return timestamp;
	}
}
