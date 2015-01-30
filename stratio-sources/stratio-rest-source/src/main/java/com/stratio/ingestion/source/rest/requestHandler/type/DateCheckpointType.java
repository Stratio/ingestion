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
package com.stratio.ingestion.source.rest.requestHandler.type;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by eambrosio on 14/01/15.
 */
public class DateCheckpointType implements CheckpointType {

    private static final String CONF_DATE_PATTERN = "format";
    private static final String ISO_8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX";
    private static final String CONF_CHECKPOINT_VALUE = "checkpointValue";

    @Override
    public String buildCheckpoint(Object fieldValue, Map<String, String> context) {
        //        Record newRecord = new Record();
        //        newRecord.put(context.get("field"), new SimpleDateFormat(getDatePattern(context)).format(fieldValue));
        //        newRecord.put("checkpoint", "true");
        //        return newRecord;
        return new SimpleDateFormat(getDatePattern(context)).format(fieldValue);
    }

    @Override
    public String buildDefaultCheckpoint(Map<String, String> context) {
        //        Record record = new Record();
        //        record.put(context.get("field"), new SimpleDateFormat(getDatePattern(context)).format(new Date(0)));
        //        return record;
        return new SimpleDateFormat(getDatePattern(context)).format(new Date(0));
    }

    @Override
    public Boolean isValidCurrentCheckpoint(Object currentCheckpoint, Map<String, String> context) {
        final String datePattern = getDatePattern(context);
        final String lastCheckpoint = context.get("lastCheckpoint");
        int isValid = 0;
        try {
            final Date lastCheckpointAsDate = new SimpleDateFormat(datePattern).parse(lastCheckpoint);
            final Date currentCheckpointAsDate = new SimpleDateFormat(datePattern).parse((String) currentCheckpoint);
            isValid = currentCheckpointAsDate.compareTo(lastCheckpointAsDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return isValid >= 0;
    }

//    @Override
//    public Object getCheckpoint(Record record, Map<String, String> context) {
//        return record.get(context.get("field")).get(0);
//    }
//
    private String getDatePattern(Map<String, String> context) {
        String pattern = context.get(CONF_DATE_PATTERN);
        if (pattern != null) {
            return pattern;
        } else {
            return ISO_8601_DATE_FORMAT;

        }
    }
}
