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

/**
 * Created by miguelsegura on 7/10/15.
 */

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * {@link Event} implementation that has a timestamp. The timestamp is taken
 * from the "@timestamp" header or set to current time if "@timestamp" is not
 * present or is invalid.
 */
public class TimeStampedEvent extends SimpleEvent {

    private static final Logger log = LoggerFactory.getLogger(TimeStampedEvent.class);

    private final long timestamp;

    TimeStampedEvent(Event base) {
        super();
        setBody(base.getBody());
        Map<String, String> headers = Maps.newHashMap(base.getHeaders());

        String timestampHeader = headers.get("@timestamp");
        Long ts = null;
        if (!StringUtils.isBlank(timestampHeader)) {
            try {
                ts = Long.parseLong(timestampHeader);
                headers.put("@timestamp", ISODateTimeFormat.dateTime().withZoneUTC().print(ts));
            } catch (RuntimeException ex) {
                log.trace("Could not parse timestamp as long: {}", timestampHeader);
                try {
                    ts = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC().parseMillis(timestampHeader);
                } catch (RuntimeException ex2) {
                    log.trace("Could not parse timestamp as dateOptionalTime: {}", timestampHeader);
                }
            }
        }

        if (ts == null) {
            DateTime now = DateTime.now();
            ts = now.getMillis();
            headers.put("@timestamp", ISODateTimeFormat.dateTime().withZoneUTC().print(now));
        }

        this.timestamp = ts;

        setHeaders(headers);
    }

    long getTimestamp() {
        return timestamp;
    }
}