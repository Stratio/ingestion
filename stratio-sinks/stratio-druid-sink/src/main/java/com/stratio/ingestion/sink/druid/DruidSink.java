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
package com.stratio.ingestion.sink.druid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.typeclass.Timestamper;
import com.twitter.finagle.Service;
import com.twitter.util.Await;
import com.twitter.util.Future;

import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;

/**
 * The Druid Sink component allows to save Flume-flow events to Druid.
 *
 * Available configuration parameters are:
 *
 * <p><ul>
 * <li><tt>indexService</tt> <em>(string, required)</em>: Overlord's service name</li>
 * <li><tt>discoveryPath</tt> <em>(string, required)</em>: Your overlord's druid.discovery.curator.path</li>
 * <li><tt>dimensions</tt> <em>(string, required)</em>:  Comma separated list with event headers you want to stored.
 * Similar to columns inrelational databases. </li>
 * <li><tt>firehosePattern</tt> <em>(string)</em>: Firehoses describe the data stream source. Make up a service pattern,
 * include %s somewhere in it. This will be used for internal service-discovery purposes, to help druid sink find Druid
 * indexing tasks. By default, *druid:firehose:%s*.</li>
 * <li><tt>dataSource</tt> <em>(string)</em>: Source name where events will be stored. Very similar to a table in relational
 * databases. By default, *sampleSource*.</li>
 * <li><tt>aggregators</tt> <em>(string)</em>: Different specifications of processing over available metrics. By default,
 * *count* aggregator.</li>
 * <li><tt>zookeeperLocation</tt> <em>(string)</em>: Zookeeper location (hostname:port).By default, *127.0.0.1:2181*.</li>
 * <li><tt>timestampField</tt> <em>(integer)</em>: The field name where event timestamp info is extracted from. By default, *timestamp*.</li>
 * <li><tt>segmentGranularity</tt> <em>(string)</em>: Time granularity (minute, hour, day, week, month) for loading data at query
 * time. Recommended, more than queryGranularity.  By default, *HOUR*.</li>
 * <li><tt>queryGranularity</tt> <em>(string)</em>: Time granularity (minute, hour, day, week, month) for rollup. At least, less
 * than segmentGranularity. Recommended: minute, hour, day, week, month.  By default, *NONE*.</li>
 * <li><tt>period</tt> <em>(string)</em>: While reading, events with timestamp older than now minus this value, will be discarded.  By
 * default, *PT10M*.</li>
 * <li><tt>partitions</tt> <em>(string)</em>: This is used to scale ingestion up to handle larger streams.  By default, *1*.</li>
 * <li><tt>replicants</tt> <em>(string, required)</em>: This is used to provide higher availability and parallelism for queries.  By default, *1*.</li>
 * <li><tt>baseSleepTime</tt> <em>(string)</em>: Initial amount of time to wait between retries.  By default, *1000*.</li>
 * <li><tt>maxRetries</tt> <em>(string)</em>: Max number of times to retry.  By default, *3*.</li>
 * <li><tt>maxSleep</tt> <em>(string)</em>: Max time in ms to sleep on each retry.  By default, *30000*.</li>
 * <li><tt>batchSize</tt> <em>(integer)</em>: Number of events to batch together to be send to our data source.  By default, *1000*.</li>
 * </ul></p>
 *
 */
public class DruidSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(DruidSink.class);

    private static final String INDEX_SERVICE = "indexService";
    private static final String FIREHOSE_PATTERN = "firehosePattern";
    private static final String DISCOVERY_PATH = "discoveryPath";
    private static final String DATA_SOURCE = "dataSource";
    private static final String DIMENSIONS = "dimensions";
    private static final String AGGREGATORS = "aggregators";
    private static final String ZOOKEEPER_LOCATION = "zookeeperLocation";
    private static final String TIMESTAMP_FIELD = "timestampField";
    private static final String SEGMENT_GRANULARITY = "segmentGranularity";
    private static final String BATCH_SIZE = "batchSize";
    public static final String QUERY_GRANULARITY = "queryGranularity";
    private static final String WINDOW_PERIOD = "period";
    private static final String PARTITIONS = "partitions";
    private static final String REPLICANTS = "replicants";
    private static final String ZOOKEEPPER_BASE_SLEEP_TIME = "baseSleepTime";
    private static final String ZOOKEEPER_MAX_RETRIES = "maxRetries";
    private static final String ZOOKEEPER_MAX_SLEEP = "maxSleep";
    private static final String DEFAULT_FIREHOSE = "druid:firehose:%s";
    private static final String DEFAUL_DATASOURCE = "sampleSource";
    private static final String DEFAULT_QUERY_GRANULARITY = "NONE";
    private static final String DEFAULT_SEGMENT_GRANULARITY = "HOUR";
    private static final String DEFAULT_PERIOD = "PT10M";
    private static final Integer DEFAULT_PARTITIONS = 1;
    private static final Integer DEFAULT_REPLICANTS = 1;
    private static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";
    private static final String DEFAULT_ZOOKEEPER_LOCATION = "localhost:2181";
    private static final Integer DEFAULT_ZOOKEEPER_BASE_SLEEP = 1000;
    private static final Integer DEFAULT_ZOOKEEPER_MAX_RETRIES = 3;
    private static final Integer DEFAULT_ZOOKEEPER_MAX_SLEEP = 30000;
    private static final Integer DEFAULT_BATCH_SIZE = 10000;

    private Service druidService;
    private CuratorFramework curator;
    private String discoveryPath;
    private String indexService;
    private String firehosePattern;

    private String dataSource;
    private TimestampSpec timestampSpec;
    private List<String> dimensions;
    private List<AggregatorFactory> aggregators;
    private SinkCounter sinkCounter;
    private Integer batchSize;
    private QueryGranularity queryGranularity;
    private Granularity segmentGranularity;
    private String period;
    private int partitions;
    private int replicants;
    private String zookeeperLocation;
    private int baseSleppTime;
    private int maxRetries;
    private int maxSleep;
    private String timestampField;
    private EventParser eventParser;

    @Override
    public void configure(Context context) {

        indexService = context.getString(INDEX_SERVICE);
        discoveryPath = context.getString(DISCOVERY_PATH);
        dimensions = Arrays.asList(context.getString(DIMENSIONS).split(","));
        firehosePattern = context.getString(FIREHOSE_PATTERN, DEFAULT_FIREHOSE);
        dataSource = context.getString(DATA_SOURCE, DEFAUL_DATASOURCE);
        aggregators = AggregatorsHelper.build(context.getString(AGGREGATORS));
        queryGranularity = QueryGranularityHelper.getGranularity(context.getString(QUERY_GRANULARITY,
                DEFAULT_QUERY_GRANULARITY));
        segmentGranularity = Granularity.valueOf(context.getString(SEGMENT_GRANULARITY, DEFAULT_SEGMENT_GRANULARITY));
        period = context.getString(WINDOW_PERIOD, DEFAULT_PERIOD);
        partitions = context.getInteger(PARTITIONS, DEFAULT_PARTITIONS);
        replicants = context.getInteger(REPLICANTS, DEFAULT_REPLICANTS);
        // Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
        timestampField = context.getString(TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
        zookeeperLocation = context.getString(ZOOKEEPER_LOCATION, DEFAULT_ZOOKEEPER_LOCATION);
        baseSleppTime = context.getInteger(ZOOKEEPPER_BASE_SLEEP_TIME, DEFAULT_ZOOKEEPER_BASE_SLEEP);
        maxRetries = context.getInteger(ZOOKEEPER_MAX_RETRIES, DEFAULT_ZOOKEEPER_MAX_RETRIES);
        maxSleep = context.getInteger(ZOOKEEPER_MAX_SLEEP, DEFAULT_ZOOKEEPER_MAX_SLEEP);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

        druidService = buildDruidService();
        sinkCounter = new SinkCounter(this.getName());
        eventParser = new EventParser(timestampField);
    }

    private Service buildDruidService() {
        curator = buildCurator();
        final TimestampSpec timestampSpec = new TimestampSpec(timestampField, "auto");
        final Timestamper<Map<String, Object>> timestamper = getTimestamper();
        final DruidLocation druidLocation = DruidLocation.create(indexService, firehosePattern, dataSource);
        final DruidRollup druidRollup = DruidRollup
                .create(DruidDimensions.specific(dimensions), aggregators, queryGranularity);
        final ClusteredBeamTuning clusteredBeamTuning = ClusteredBeamTuning.builder()
                .segmentGranularity(segmentGranularity)
                .windowPeriod(new Period(period)).partitions(partitions).replicants(replicants).build();//TODO revise

        return DruidBeams.builder(timestamper).curator(curator).discoveryPath(discoveryPath).location(
                druidLocation).timestampSpec(timestampSpec).rollup(druidRollup).tuning(clusteredBeamTuning)
                .buildJavaService();
    }

    @Override
    public Status process() throws EventDeliveryException {
        List<Event> events;
        List<Map<String, Object>> parsedEvents;
        Status status = Status.BACKOFF;
        Transaction transaction = this.getChannel().getTransaction();
        try {
            transaction.begin();
            events = takeEventsFromChannel(this.getChannel(), batchSize);
            status = Status.READY;
            if (!events.isEmpty()) {
                updateSinkCounters(events);
                parsedEvents = eventParser.parse(events);
                sendEvents(parsedEvents);
                sinkCounter.addToEventDrainSuccessCount(events.size());
            } else {
                sinkCounter.incrementBatchEmptyCount();
            }
            transaction.commit();
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
            this.sinkCounter.incrementConnectionFailedCount();
        } catch (Throwable t) {
            t.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                LOG.error(t.getMessage());
                throw new DruidSinkException("An error occurred during processing events to be stored in druid", t);

            }
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override public synchronized void start() {
        this.sinkCounter.start();

        super.start();
    }

    private void updateSinkCounters(List<Event> events) {
        if (events.size() == batchSize) {
            sinkCounter.incrementBatchCompleteCount();
        } else {
            sinkCounter.incrementBatchUnderflowCount();
        }
    }

    private List<Event> takeEventsFromChannel(Channel channel, long eventsToTake) throws ChannelException {
        List<Event> events = new ArrayList<Event>();
        Event event;
        for (int i = 0; i < eventsToTake; i++) {
            event = buildEvent(channel);
            events.add(event);
            if (event != null) {
                sinkCounter.incrementEventDrainAttemptCount();
            }
        }
        events.removeAll(Collections.singleton(null));
        return events;
    }

    private Event buildEvent(Channel channel) {
        final Event takenEvent = channel.take();
        final ObjectNode objectNode = new ObjectNode(JsonNodeFactory.instance);
        Event event = null;
        if (takenEvent != null) {
            event = EventBuilder.withBody(objectNode.toString().getBytes(Charsets.UTF_8),
                    takenEvent.getHeaders());
        }
        return event;
    }

    private int sendEvents(List<Map<String, Object>> events) {
        int sentEvents = 0;
        // Send events to Druid:
        final Future<Integer> numSentFuture = druidService.apply(events);

        // Wait for confirmation:
        try {
            sentEvents = Await.result(numSentFuture);
        } catch (Exception e) {
            throw new DruidSinkException("An error occurred during sending events to druid", e);
        }

        return sentEvents;
    }

    private CuratorFramework buildCurator() {
        // Tranquility uses ZooKeeper (through Curator) for coordination.
        final CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString(zookeeperLocation)
                .retryPolicy(new ExponentialBackoffRetry(baseSleppTime, maxRetries, maxSleep))
                .build();
        curator.start();

        return curator;
    }

    private Timestamper<Map<String, Object>> getTimestamper() {
        return new Timestamper<Map<String, Object>>() {
            @Override
            public DateTime timestamp(Map<String, Object> theMap) {
                return new DateTime(theMap.get(timestampField));
            }
        };
    }
}
