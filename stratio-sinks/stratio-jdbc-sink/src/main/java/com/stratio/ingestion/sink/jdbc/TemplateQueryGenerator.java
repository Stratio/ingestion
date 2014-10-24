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

import com.google.common.base.*;
import org.apache.flume.Event;
import org.jooq.*;
import org.jooq.impl.DefaultDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class TemplateQueryGenerator implements QueryGenerator {

    private static final Logger log = LoggerFactory.getLogger(TemplateQueryGenerator.class);

    private static final Pattern PARAMETER_PATTERN = Pattern.compile("\\$\\{(?<part>[^\\s.{}]+)(?:\\.(?<header>[^\\s.{}]+))?:(?<type>[^\\s.{}]+)\\}");

    private static final String BODY = "BODY";
    private static final String HEADER = "HEADER";

    private final List<Parameter> parameters;

    final String sql;

    public TemplateQueryGenerator(final SQLDialect sqlDialect, final String sql) {
        final Matcher m = PARAMETER_PATTERN.matcher(sql);

        parameters = new ArrayList<Parameter>();

        while (m.find()) {
            final String part = m.group("part").toUpperCase(Locale.ENGLISH);
            final String header = m.group("header");
            final String type = m.group("type").toUpperCase(Locale.ENGLISH);
            DataType<?> dataType = DefaultDataType.getDataType(sqlDialect, type);
            if (BODY.equals(part)) {
                if (header != null) {
                    throw new IllegalArgumentException("BODY parameter must have no header name specifier (${body:" + type + "}, not  (${body." + header + ":" + type + "}");
                }
            }
            final Parameter parameter = new Parameter(header, dataType);
            parameters.add(parameter);
            log.trace("Parameter: {}", parameter);
        }

        this.sql = m.replaceAll("?");
        log.debug("Generated SQL: {}", this.sql);
    }

    public boolean executeQuery(final DSLContext dslContext, final List<Event> events) {
        List<Query> queries = new ArrayList<Query>();
        for (int i = 0; i < events.size(); i++) {
            final Object[] bindings = new Object[this.parameters.size()];
            for (int j = 0; j < this.parameters.size(); j++) {
                bindings[j] = this.parameters.get(j).binding(events.get(i));
            }
            queries.add(dslContext.query(this.sql, bindings));
        }
        dslContext.batch(queries).execute();
        return true;
    }

    private static class Parameter {

        private final String header;
        private final DataType<?> dataType;

        public Parameter(final String header, final DataType<?> dataType) {
            this.header = header;
            this.dataType = dataType;
        }

        public Object binding(final Event event) {
            if (header == null) {
                final byte body[] = event.getBody();
                return dataType.convert(new String(body, Charsets.UTF_8));
            } else {
                final Map<String, String> headers = event.getHeaders();
                for (final Map.Entry<String, String> entry : headers.entrySet()) {
                    if (entry.getKey().equals(header)) {
                        return dataType.convert(entry.getValue());
                    }
                }
            }
            log.trace("No bindable field found for {}", this);
            return null;
        }

        @Override
        public String toString() {
            return com.google.common.base.Objects.toStringHelper(Parameter.class)
                    .add("header", header).add("dataType", dataType).toString();
        }
    }

}
