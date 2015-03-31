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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

// @formatter:off

/**
 * The headersToBody is a command that write in your _attachment_body field all your headers but excluded in JSON
 * format.  Warning: This morphline REPLACE your _attachment_body field but maintains the headers.
 * {
      {
        headersToBody {
          excludeFields : [field2, field5]
      }
   }
 */
// @formatter:on
public class HeadersToBodyBuilder implements CommandBuilder {

    private static final String COMMAND_NAME = "headersToBody";
    private static final String CONF_EXCLUDE_FIELDS = "excludeFields";

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList(COMMAND_NAME);
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new HeadersToBody(this, config, parent, child, context);
    }

    private static final class HeadersToBody extends AbstractCommand {


        private List<String> excludeFields;
        private ObjectMapper mapper;

        protected HeadersToBody(CommandBuilder builder, Config config, Command parent, Command child,
                MorphlineContext context) {

            super(builder, config, parent, child, context);

            this.excludeFields = getConfigs().getStringList(config, CONF_EXCLUDE_FIELDS, ImmutableList.<String>of());
            this.excludeFields.add(Fields.ATTACHMENT_BODY);
            this.mapper = new ObjectMapper();
        }

        @Override
        protected boolean doProcess(Record record) {

            Map<String, Object> map = new HashMap<String, Object>();

            for(String key : record.getFields().keySet()){
                if(!excludeFields.contains(key)){
                    map.put(key, record.getFirstValue(key));
                }
            }

            try {
                record.replaceValues(Fields.ATTACHMENT_BODY, mapper.writeValueAsString(map));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            return super.doProcess(record);
        }

        private List<String> parseSepparatedCommaList(String line){
            String[] splits = line.split(",");
            List<String> list = new ArrayList();
            Collections.addAll(list, splits);
            return list;
        }
    }
}
