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
package com.stratio.ingestion.source.batch.rest.request;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.ingestion.source.batch.rest.checkpoint.CheckpointRetrieval;
import com.sun.jersey.api.client.Client;

/**
 * Make a request to a RESTful Service.
 * <p/>
 * Configuration parameters are:
 * <p/>
 * <p>
 * <ul>
 * <li><tt>url</tt> <em>(string, required)</em>: target URI.</li>
 * <li><tt>frequency</tt> <em>(integer)</em>: Frequency, in seconds, to make the request. Default:
 * 10 seconds.</li>
 * <li><tt>method</tt> <em>(string)</em>: Method type. Possible values: GET, POST. Default: GET.</li>
 * <li><tt>applicationType</tt> <em>(string)</em>: Application Type. Possible values: JSON, TEXT.
 * Default: JSON.</li>
 * <li><tt>headers</tt> <em>(string)</em>: Headers json. e.g.: { Accept-Charset: utf-8, Date: Tue,
 * 15 Nov 1994 08:12:31 GMT} Default: "".</li>
 * <li><tt>body</tt> <em>(string)</em>: Body for post request. Default: "".</li>
 * </ul>
 * </p>
 */
public class RestSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger log = LoggerFactory.getLogger(RestSource.class);
    private static final Integer QUEUE_SIZE = 10;

    private static final Integer DEFAULT_FREQUENCY = 10;
    private static final String DEFAULT_JOBNAME = "Quartz Job";
    private static final String DEFAULT_METHOD = "GET";
    private static final String DEFAULT_APPLICATION_TYPE = "JSON";
    private static final String DEFAULT_HEADERS = "{}";
    private static final String DEFAULT_BODY = "";

    protected static final String CONF_FREQUENCY = "frequency";
    protected static final String CONF_URL = "url";
    protected static final String CONF_METHOD = "method";
    protected static final String CONF_APPLICATION_TYPE = "applicationType";
    protected static final String CONF_HEADERS = "headers";
    protected static final String CONF_BODY = "body";
    protected static final String CONF_DATE_PATTERN = "datePattern";
    protected static final String CONF_CHECKPOINT_VALUE = "checkpointValue";
    protected static final String CONF_CHECKPOINT_ENABLED = "checkpointEnabled";
    protected static final String ISO_8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX";
    protected static final String CONF_CHECKPOINT_RETRIEVAL = "checkpointRetrieval";

    private LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(QUEUE_SIZE);
    private int frequency;
    private final Client client;
    private JobDetail jobDetail;
    private Scheduler scheduler;
    private Map<String, String> properties = new HashMap<>();
    private CheckpointRetrieval checkpointRetrival;

    public RestSource() {
        client = new Client();
    }

    public RestSource(Client client) {
        this.client = client;
    }

    /**
     * {@inheritDoc}
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        frequency = context.getInteger(CONF_FREQUENCY, DEFAULT_FREQUENCY);

        properties.put(CONF_URL, context.getString(CONF_URL));
        properties.put(CONF_METHOD, context.getString(CONF_METHOD, DEFAULT_METHOD).toUpperCase());
        properties.put(CONF_APPLICATION_TYPE,
                context.getString(CONF_APPLICATION_TYPE, DEFAULT_APPLICATION_TYPE).toUpperCase());
        properties.put(CONF_HEADERS, context.getString(CONF_HEADERS, DEFAULT_HEADERS));
        properties.put(CONF_BODY, context.getString(CONF_BODY, DEFAULT_BODY));

        initCheckpointRetrieval(context);

    }

    private void initCheckpointRetrieval(Context context) {
        final Boolean isCheckpointEnabled = new Boolean(context.getString(CONF_CHECKPOINT_ENABLED, "false"));
        Class<?> retrievalClass = null;
        if (isCheckpointEnabled) {
            try {
                retrievalClass = Class.forName(context.getString(CONF_CHECKPOINT_RETRIEVAL));
                checkpointRetrival = (CheckpointRetrieval) retrievalClass.getDeclaredConstructor(Context.class)
                        .newInstance(context);

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
            properties.put(CONF_CHECKPOINT_ENABLED, isCheckpointEnabled.toString());
            properties.put(CONF_DATE_PATTERN, context.getString(CONF_DATE_PATTERN, ISO_8601_DATE_FORMAT));
            getLastCheckpoint(context);
        }
    }

    private void getLastCheckpoint(Context context) {
        final Object lastCheckpoint = checkpointRetrival.getLastCheckpoint();

        if (lastCheckpoint != null) {
            if (lastCheckpoint instanceof Date) {
                properties.put(CONF_CHECKPOINT_VALUE, new SimpleDateFormat(properties.get(CONF_DATE_PATTERN)).format(
                        lastCheckpoint));
            }
        } else {
            properties.put(CONF_CHECKPOINT_VALUE, context.getString(CONF_CHECKPOINT_VALUE, String.valueOf(0)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        this.jobDetail = JobBuilder.newJob(RequestJob.class).withIdentity(DEFAULT_JOBNAME).build();

        // Create an scheduled trigger with interval in seconds
        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity(DEFAULT_JOBNAME)
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(frequency)
                                .repeatForever()).build();

        // Put needed object in scheduler context and start the job.
        try {
            scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.getContext().put("client", client);
            scheduler.getContext().put("queue", queue);
            scheduler.getContext().put("properties", properties);
            scheduler.start();
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

        try {
            Event e = poll();
            if (e != null) {
                getChannelProcessor().processEvent(e);
                status = Status.READY;
            }
        } catch (Throwable t) {
            status = Status.BACKOFF;
            log.error("RestSource error. " + t.getMessage());
        }

        return status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        client.destroy();
        try {
            scheduler.interrupt(jobDetail.getKey());
        } catch (UnableToInterruptJobException e) {
            e.printStackTrace();
        }
    }

    /**
     * Look at the queue and poll and {@code Event}
     *
     * @return an {@code Event} or null if is empty.
     */
    private Event poll() {
        if (!queue.isEmpty()) {
            return queue.poll();
        } else {
            try {
                Thread.sleep(frequency * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

}
