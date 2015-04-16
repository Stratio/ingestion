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
import io.druid.query.aggregation.CountAggregatorFactory;

/**
 * Created by eambrosio on 27/03/15.
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
    private static final Integer DEFAULT_BATCH_SIZE = 10000;
    public static final String QUERY_GRANULARITY = "queryGranularity";
    private static final String WINDOW_PERIOD = "period";
    private static final String PARTITIONS = "partitions";
    private static final String REPLICANTS = "replicants";
    private static final String ZOOKEEPPER_BASE_SLEEP_TIME = "baseSleepTime";
    private static final String ZOOKEEPER_MAX_RETRIES = "maxRetries";
    private static final String ZOOKEEPER_MAX_SLEEP = "maxSleep";

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
        firehosePattern = context.getString(FIREHOSE_PATTERN);
        dataSource = context.getString(DATA_SOURCE);
        dimensions = Arrays.asList(context.getString(DIMENSIONS).split(","));
        //        aggregators = AggregatorsHelper.build(Arrays.asList(context.getString(AGGREGATORS).split(",")));
        aggregators = new ArrayList<AggregatorFactory>();
        aggregators.add(new CountAggregatorFactory("count"));
        queryGranularity = QueryGranularityHelper.getGranularity(context.getString(QUERY_GRANULARITY));
        segmentGranularity = Granularity.valueOf(context.getString(SEGMENT_GRANULARITY));
        period = context.getString(WINDOW_PERIOD);
        partitions = context.getInteger(PARTITIONS);
        replicants = context.getInteger(REPLICANTS);
        // Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
        discoveryPath = context.getString(DISCOVERY_PATH);
        timestampField = context.getString(TIMESTAMP_FIELD);
        zookeeperLocation = context.getString(ZOOKEEPER_LOCATION);
        baseSleppTime = context.getInteger(ZOOKEEPPER_BASE_SLEEP_TIME);
        maxRetries = context.getInteger(ZOOKEEPER_MAX_RETRIES);
        maxSleep = context.getInteger(ZOOKEEPER_MAX_SLEEP);

        druidService = buildDruidService();
        sinkCounter = new SinkCounter(this.getName());
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        eventParser = new EventParser(timestampField);
    }

    private Service buildDruidService() {
        //        curator = buildCurator();
        //        final TimestampSpec timestampSpec = new TimestampSpec(timestampField, "auto");
        //        final Timestamper<Map<String, Object>> timestamper = getTimestamper();
        //        final DruidLocation druidLocation = DruidLocation.create(indexService, firehosePattern, dataSource);
        //        final DruidRollup druidRollup = DruidRollup
        //                .create(DruidDimensions.specific(dimensions), aggregators, queryGranularity);
        //        final ClusteredBeamTuning clusteredBeamTuning = ClusteredBeamTuning.builder()
        //                .segmentGranularity(segmentGranularity)
        //                .windowPeriod(new Period(period)).partitions(partitions).replicants(replicants).build();//TODO revise
        //
        //        return DruidBeams.builder(timestamper).curator(curator).discoveryPath(discoveryPath).location(
        //                druidLocation).timestampSpec(timestampSpec).rollup(druidRollup).tuning(clusteredBeamTuning)
        //                .buildJavaService();

        final Timestamper<Map<String, Object>> timestamper = new Timestamper<Map<String, Object>>() {
            @Override
            public DateTime timestamp(Map<String, Object> theMap) {
                return new DateTime(theMap.get("timestamp"));
            }
        };
        curator = CuratorFrameworkFactory
                .builder()
                .connectString("druid-server:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
                .build();
        curator.start();
        return DruidBeams.builder(timestamper)
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(
                        DruidLocation.create(
                                indexService,
                                firehosePattern,
                                dataSource
                        )
                )
                .timestampSpec(new TimestampSpec("timestamp", "auto"))
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, queryGranularity))
                .tuning(
                        ClusteredBeamTuning
                                .builder()
                                .segmentGranularity(segmentGranularity)
                                .windowPeriod(new Period("PT1M"))
                                .partitions(1)
                                .replicants(1)
                                .build()
                )
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

    @Override public synchronized void stop() {
        // Close lifecycled objects:
        try {
            Await.result(druidService.close());
        } catch (Exception e) {
            e.printStackTrace();
        }
        curator.close();
        super.stop();
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
                return new DateTime(theMap.get("timestamp"));
            }
        };
    }
}
