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
package com.stratio.ingestion.source.batch.rest.checkpoint;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.flume.Context;

/**
 * Created by eambrosio on 7/01/15.
 */
public class DateCheckpointType implements CheckpointType {

    private static final String CONF_DATE_PATTERN = "datePattern";
    private static final String ISO_8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX";

    @Override
    public String populateCheckpoint(Object checkpoint, Context context) {
        return new SimpleDateFormat(context.getString(CONF_DATE_PATTERN, ISO_8601_DATE_FORMAT)).format(checkpoint);
    }

    @Override
    public String populateDefaultCheckpoint(Context context) {
        return new SimpleDateFormat(context.getString(CONF_DATE_PATTERN, ISO_8601_DATE_FORMAT)).format(new Date(0));
    }

    @Override
    public Boolean isValid(Object lastCheckpoint, Object currentCheckpoint, Map<String,String> properties) {
        final String datePattern = properties.get(CONF_DATE_PATTERN);
        int isValid = 0;
        try {
            final Date lastCheckpointAsDate = new SimpleDateFormat(datePattern).parse((String)lastCheckpoint);
            final Date currentCheckpointAsDate = new SimpleDateFormat(datePattern).parse((String)currentCheckpoint);
            isValid = currentCheckpointAsDate.compareTo(lastCheckpointAsDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return isValid>=0;
    }

}
