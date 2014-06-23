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
package com.stratio.ingestion.sink.jdbc;

import org.apache.flume.Event;
import org.jooq.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

class MappingQueryGenerator implements QueryGenerator {

    private static final Logger log = LoggerFactory.getLogger(MappingQueryGenerator.class);

    private Table table;

    public MappingQueryGenerator(DSLContext dslContext, final String tableName) {
        Meta meta = dslContext.meta();

        for (Table table : meta.getTables()) {
            System.out.println(table.getName());
            if (table.getName().equalsIgnoreCase(tableName)) {
                this.table = table;
                break;
            }
        }
        if (this.table == null) {
            throw new JDBCSinkException("Table not found: " + tableName);
        }
    }

    public boolean executeQuery(DSLContext dslContext, final List<Event> events) {
        InsertSetStep insert = dslContext.insertInto(this.table);
        int mappedEvents = 0;
        for (Event event : events) {
            Map<Field, Object> fieldValues = new HashMap<>();
            for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
                Field field = null;
                for (Field f: this.table.fields()) {
                    if (f.getName().equalsIgnoreCase(entry.getKey())) {
                        field = f;
                        break;
                    }
                }
                if (field == null) {
                    log.trace("Ignoring field: {}", entry.getKey());
                    continue;
                }
                DataType dataType = field.getDataType();
                fieldValues.put(field, dataType.convert(entry.getValue()));
            }
            if (fieldValues.isEmpty()) {
                log.debug("Ignoring event, no mapped fields.");
            } else {
                mappedEvents++;
                if (insert instanceof InsertSetMoreStep) {
                    insert = ((InsertSetMoreStep) insert).newRecord();
                }
                for (Map.Entry<Field, Object> entry : fieldValues.entrySet()) {
                    insert = insert.set(entry.getKey(), entry.getValue());
                }
            }
        }
        if (insert instanceof InsertSetMoreStep) {
            int result = ((InsertSetMoreStep) insert).execute();
            if (result != mappedEvents) {
                log.warn("Mapped {} events, inserted {}.", mappedEvents, result);
                return false;
            }
        } else {
            log.debug("No insert.");
        }
        return true;
    }

}
