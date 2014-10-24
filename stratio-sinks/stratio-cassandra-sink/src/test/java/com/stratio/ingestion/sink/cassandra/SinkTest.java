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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SinkCounter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Charsets;

@RunWith(MockitoJUnitRunner.class)
public class SinkTest {

	private String body1 = "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245";
	private Map<String, String> headers1 = new HashMap<String, String>();
	private String body2 = "unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985";
	private Map<String, String> headers2 = new HashMap<String, String>();

	@Mock
	private MemoryChannel channel;
	private CassandraSink sink;

	@Mock
	private CassandraRepository sinkRepository;
	@Mock
	private EventParser parser;

	private ArrayList<Event> eventList = new ArrayList<Event>();
	private SinkCounter sinkCounter;

	@Before
	public void before() throws IOException {
		Context channelContext = new Context();
		channelContext.put("capacity", "10000");
		channelContext.put("transactionCapacity", "200");

		channel = new MemoryChannel();
		channel.setName("junitChannel");
		Configurables.configure(channel, channelContext);

		this.parser = new EventParser(IOUtils.toString(this.getClass()
				.getResourceAsStream("/definition.json")));

		headers1.put("log_id", "38400000-8cf0-11bd-b23e-10b96e4ef00d");
		headers1.put("log_host", "199.72.81.55");
		headers2.put("log_id", "38400000-8cf0-11bd-b23e-10b96e4ef00e");
		headers2.put("log_host", "unicomp6.unicomp.net");

		eventList.add(EventBuilder.withBody(body1, Charsets.UTF_8, headers1));
		eventList.add(EventBuilder.withBody(body2, Charsets.UTF_8, headers2));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcess() throws Exception {
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		for (Event event : eventList) {
			channel.put(event);
		}
		transaction.commit();
		transaction.close();

		this.sinkRepository = Mockito.mock(CassandraRepository.class);
		this.sinkCounter = Mockito.mock(SinkCounter.class);
		this.parser = Mockito.mock(EventParser.class);
		Mockito.doNothing().when(sinkRepository).save(Mockito.anyList());
		List<CassandraRow> rowList = new ArrayList<CassandraRow>(3);
		Mockito.when(parser.parse(eventList)).thenReturn(rowList);
		this.sink = new CassandraSink(channel, sinkCounter, sinkRepository, 3,
				parser);
		sink.process();
		Mockito.verify(sinkRepository, Mockito.times(1))
				.save(Mockito.anyList());
	}
}