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
package com.stratio.ingestion.source.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

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
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Make a request to a RESTful Service.
 * 
 * Configuration parameters are:
 * 
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
 * 
 */
public class RestSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger log = LoggerFactory.getLogger(RestSource.class);
    private static final Integer QUEUE_SIZE = 10;

    private static final Integer DEFAULT_FREQUENCY = 10;
    private static final String DEFAULT_JOBNAME = "Quartz Job";
    private static final String DEFAULT_METHOD = "GET";
    private static final String DEFAULT_APPLICATION_TYPE = "JSON";
    private static final String DEFAULT_HEADERS = "";
    private static final String DEFAULT_BODY = "";

    private static final String CONF_FREQUENCY = "frequency";
    private static final String CONF_URL = "url";
    private static final String CONF_METHOD = "method";
    private static final String CONF_APPLICATION_TYPE = "applicationType";
    private static final String CONF_HEADERS = "headers";
    private static final String CONF_BODY = "body";

    private LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(QUEUE_SIZE);
    private int frequency;
    private Client client;
    private Map<String, Object> properties = new HashMap<String, Object>();

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

        if (queue != null) {
            log.error("QUEUE ISNOT NULL");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        client = ClientBuilder.newClient();
        JobDetail job = JobBuilder.newJob(RequestJob.class).withIdentity(DEFAULT_JOBNAME).build();

        // Create an scheduled trigger with interval in seconds
        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity(DEFAULT_JOBNAME)
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(frequency)
                                .repeatForever()).build();

        // Put needed object in scheduler context and start the job.
        Scheduler scheduler;
        try {
            scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.getContext().put("client", client);
            scheduler.getContext().put("queue", queue);
            scheduler.getContext().put("properties", properties);
            scheduler.start();
            scheduler.scheduleJob(job, trigger);
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
        client.close();
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
