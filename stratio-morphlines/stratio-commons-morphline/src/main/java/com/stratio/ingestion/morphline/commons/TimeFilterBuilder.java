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
package com.stratio.ingestion.morphline.commons;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

//@formatter:off
/**
 * <p>The timeFilter command discard records that are not between two specified dates.</p>
 * <ul>
 * <li><em>field</em>: Name of field which include date value.</li>
 * <li><em>dateFormat</em>: Pattern for a date. See <a href="http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html">SimpleDateFormat</a>.
 * <li><em>from</em>: Initial date. (Exclusive).</li>
 * <li><em>to</em>: End date. (Exclusive).</li>
 * </ul>
 * <code>
 * Example: 
 * { 
 *     timeFilter {
 *         field : createdAt
 *         dateFormat : "dd/MM/yyyy"
 *         from : "20/01/2014"
 *         to : "20/01/2015"
 *     } 
 * }
 * </code>
 */
//@formatter:on
public class TimeFilterBuilder implements CommandBuilder {

    private static final String COMMAND_NAME = "timeFilter";

    private static final String CONF_DATEFORMAT = "dateFormat";
    private static final String CONF_TIMEZONE = "timezone";
    private static final String CONF_FROM = "from";
    private static final String CONF_TO = "to";
    private static final String CONF_FIELD = "field";

    private static final String DEFAULT_DATEFORMAT = "dd/MM/yyyy";
    private static final String DEFAULT_TIMEZONE = "GMT";
    private static final String DEFAULT_FIELD = "timestamp";

    @Override
    public Collection<String> getNames() {
        return Collections.singleton(COMMAND_NAME);
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new TimeFilter(this, config, parent, child, context);
    }

    private static final class TimeFilter extends AbstractCommand {

        private final String dateFormat;
        private final String timeZone;
        private final String field;
        private final String fromDate;
        private final String toDate;
        private DateFormat formatter;

        protected TimeFilter(CommandBuilder builder, Config config, Command parent, Command child,
                MorphlineContext context) {
            super(builder, config, parent, child, context);

            dateFormat = getConfigs().getString(config, CONF_DATEFORMAT, DEFAULT_DATEFORMAT);
            timeZone = getConfigs().getString(config, CONF_TIMEZONE, DEFAULT_TIMEZONE);
            fromDate = getConfigs().getString(config, CONF_FROM);
            toDate = getConfigs().getString(config, CONF_TO);
            field = getConfigs().getString(config, CONF_FIELD, DEFAULT_FIELD);

            formatter = new SimpleDateFormat(dateFormat);
            formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
        }

        @Override
        protected boolean doProcess(Record record) {

            String value = String.valueOf(record.getFirstValue(field));

            Date from, to, current;
            try {
                current = formatter.parse(value);

                if (fromDate == null || toDate == null) {
                    LOG.error("Properties fromDate and toDate must be set");
                    return false;
                }

                from = formatter.parse(fromDate);
                to = formatter.parse(toDate);
                if (from.before(current) && to.after(current)) {
                    return super.doProcess(record);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

            return true;
        }
    }

}
