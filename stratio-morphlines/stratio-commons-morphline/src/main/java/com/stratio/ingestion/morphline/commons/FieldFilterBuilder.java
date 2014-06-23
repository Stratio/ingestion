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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.typesafe.config.Config;
import org.kitesdk.morphline.api.*;
import org.kitesdk.morphline.base.AbstractCommand;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class FieldFilterBuilder implements CommandBuilder {

    private static final String COMMAND_NAME = "fieldFilter";

    private static final String CONF_EXCLUDE_FIELDS = "excludeFields";
    private static final String CONF_INCLUDE_FIELDS = "includeFields";
    private static final String CONF_IS_REGEX = "isRegex";
    private static final List<String> DEFAULT_EXCLUDE_FIELDS = ImmutableList.of();
    private static final List<String> DEFAULT_INCLUDE_FIELDS = ImmutableList.of();
    private static final boolean DEFAULT_IS_REGEX = false;

    @Override
    public Collection<String> getNames() {
        return Collections.singleton(COMMAND_NAME);
    }

    @Override
    public Command build(final Config config, final Command parent,
                         final Command child, final MorphlineContext context) {
        return new FieldFilter(this, config, parent, child, context);
    }

    private static final class FieldFilter extends AbstractCommand {

        private final List<String> excludeFields;
        private final List<String> includeFields;
        private final Pattern excludePattern;
        private final Pattern includePattern;
        private final boolean isRegex;

        public FieldFilter(final CommandBuilder builder, final Config config, final Command parent,
                           final Command child, final MorphlineContext context) {
            super(builder, config, parent, child, context);

            this.excludeFields = getConfigs().getStringList(config, CONF_EXCLUDE_FIELDS, DEFAULT_EXCLUDE_FIELDS);
            this.includeFields = getConfigs().getStringList(config, CONF_INCLUDE_FIELDS, DEFAULT_INCLUDE_FIELDS);
            if (excludeFields.isEmpty() && includeFields.isEmpty()) {
                throw new MorphlineCompilationException(String.format("Either %s or %s must be set", CONF_EXCLUDE_FIELDS, CONF_INCLUDE_FIELDS), config);
            }
            this.isRegex = getConfigs().getBoolean(config, CONF_IS_REGEX, DEFAULT_IS_REGEX);
            if (this.isRegex) {
                if (!excludeFields.isEmpty()) {
                    excludePattern = Pattern.compile(String.format("(%s)", Joiner.on('|').join(excludeFields)));
                } else {
                    excludePattern = Pattern.compile(".*");
                }
                if (!includeFields.isEmpty()) {
                    includePattern = Pattern.compile(String.format("(%s)", Joiner.on('|').join(includeFields)));
                } else {
                    includePattern = Pattern.compile(".*");
                }
            } else {
                includePattern = null;
                excludePattern = null;
            }
        }

        @Override
        protected boolean doProcess(Record record) {
            final ListMultimap<String, Object> entries = record.getFields();
            List<String> toRemove = new ArrayList<>();
            if (this.isRegex) {
                for (final String field : entries.keySet()) {
                    if (!includeFields.isEmpty() && includePattern.matcher(field).matches()) {
                        continue;
                    }
                    if (excludePattern.matcher(field).matches()) {
                        toRemove.add(field);
                    }
                }
            } else {
                for (final String field : entries.keySet()) {
                    if (!includeFields.isEmpty() && includeFields.contains(field)) {
                        continue;
                    }
                    if (excludeFields.isEmpty() || excludeFields.contains(field)) {
                        toRemove.add(field);
                    }
                }
            }
            for (final String field : toRemove) {
                record.removeAll(field);
            }
            return super.doProcess(record);
        }


    }
}
