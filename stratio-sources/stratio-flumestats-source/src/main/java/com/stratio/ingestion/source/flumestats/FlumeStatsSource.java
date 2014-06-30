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
package com.stratio.ingestion.source.flumestats;

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
 * Make a request to Flume Stats server and send an event for each component.
 * 
 * Configuration parameters are:
 * 
 * <p>
 * <ul>
 * <li><tt>host</tt> <em>(string)</em>: Flume webserver host. Default: localhost</li>
 * <li><tt>port</tt> <em>(integer)</em>: Flume webserver port. Default: 41414</li>
 * <li><tt>frequency</tt> <em>(integer)</em>: Frequency, in seconds, to make the request. Default:
 * 10 seconds.</li>
 * </ul>
 * </p>
 * 
 */
public class FlumeStatsSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger log = LoggerFactory.getLogger(FlumeStatsSource.class);
    private static final Integer QUEUE_SIZE = 1000;

    private static final Integer DEFAULT_FREQUENCY = 1000;
    private static final String DEFAULT_JOBNAME = "Quartz Job";
    private static final String DEFAULT_HOST= "localhost";
    private static final Integer DEFAULT_PORT= 41414;
    
    private static final String CONF_FREQUENCY = "frequency";
    private static final String CONF_HOST = "host";
    private static final String CONF_PORT = "port";

    private LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(QUEUE_SIZE);
    private int frequency;
    private String url;
    private Client client;

    /**
     * {@inheritDoc}
     * 
     * @param context
     */
    @Override
    public void configure(Context context) {
        frequency = context.getInteger(CONF_FREQUENCY, DEFAULT_FREQUENCY);
        String host = context.getString(CONF_HOST, DEFAULT_HOST);
        Integer port = context.getInteger(CONF_PORT, DEFAULT_PORT);
        url = "http://" + host + ":" + port + "/metrics";
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
            scheduler.getContext().put("url", url);
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
