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
package com.stratio.ingestion.morphline.checkpointfilter.builder;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.stratio.ingestion.morphline.checkpointfilter.handler.CheckpointFilterHandler;
import com.stratio.ingestion.morphline.checkpointfilter.type.CheckpointType;
import com.typesafe.config.Config;

//@formatter:off
/**
 * <p>The checkpointFilter command discard records that are less than specified checkpoint field value.</p>
 * <ul>
 * <li><em>handler</em></li>
 * <li><em>field</em></li>
 * <li><em>type</em></li>
 * <li><em>format</em></li>
 * <li><em>chunksize</em></li>
 * </ul>
 * <code>
 * Example:
 * {
 *     checkpointFilter {
 *         handler: com.stratio.ingestion.morphline.checkpointfilter.handler.MongoCheckpointHandler
 *         field: date
 *         type: com.stratio.ingestion.morphline.checkpointfilter.type.DateCheckpointType
 *         format: "dd/MM/yyyy"
 *         chunksize: 1000
 *         mongoUri: mongodb://127.0.0.1:27017/mydb.checkpoints
 *     }
 * }
 * </code>
 */
//@formatter:on
public class CheckpointFilterBuilder extends AbstractCheckpointFilter implements CommandBuilder {

    private static final String COMMAND_NAME = "checkpointFilter";
    private static final String CONF_HANDLER = "handler";
    private static final String CONF_FIELD = "field";
    private static final String CONF_TYPE = "type";
    private static final String CONF_FORMAT = "format";
    private static final String CONF_CHUNKSIZE = "chunksize";
    private static final String CONF_MONGO_URI = "mongoUri";

    @Override public Collection<String> getNames() {
        return Collections.singleton(COMMAND_NAME);
    }

    @Override public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new CheckpointFilter(this, config, parent, child, context);
    }

    private static final class CheckpointFilter extends AbstractCommand {

        private final CheckpointFilterHandler handler;
        private final String field;
        private final CheckpointType type;
        private final String format;
        private final int chunksize;
        private final String mongoUri;
        private String lastCheckpoint;

        private Map<String, String> filterContext;
        private int processedRecordsCounter = 0;

        public CheckpointFilter(CommandBuilder builder, Config config, Command parent,
                Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);

            field = getConfigs().getString(config, CONF_FIELD);
            format = getConfigs().getString(config, CONF_FORMAT);
            chunksize = Integer.valueOf(getConfigs().getString(config, CONF_CHUNKSIZE));
            mongoUri = getConfigs().getString(config, CONF_MONGO_URI);
            initContext(config);
            type = getTypeInstance(filterContext);
            handler = getHandlerInstance(type, filterContext);
            lastCheckpoint = handler.getLastCheckpoint(filterContext);
            filterContext.put("lastCheckpoint", lastCheckpoint);
        }

        @Override
        protected void doNotify(Record notification) {
            super.doNotify(notification);
        }

        @Override
        protected boolean doProcess(Record record) {
            if (type.isValidCurrentCheckpoint(getField(record), filterContext)) {
                return processRecord(record);
            }
            return true;
        }

        private Object getField(Record record) {
            final ObjectNode attachment_body = (ObjectNode) record.get("_attachment_body").get(0);
            return attachment_body.get(field).asText();
        }

        private boolean processRecord(Record record) {
            processedRecordsCounter++;
            if (processedRecordsCounter >= chunksize) {
                processedRecordsCounter = 0;
                filterContext.put("lastCheckpoint", (String) type.getCheckpoint(record, filterContext));
                record.put("checkpoint", "true");
            }
            return super.doProcess(record);
        }

        private void initContext(Config config) {
            filterContext = new HashMap<String, String>(ImmutableMap.<String, String>builder()
                    .put("field", field)
                    .put("format", format)
                    .put("chunksize", String.valueOf(chunksize))
                    .put("mongoUri", mongoUri)
                    .put("type", getConfigs().getString(config, CONF_TYPE))
                    .put("handler", getConfigs().getString(config, CONF_HANDLER))
                    .build());
        }
    }
}
