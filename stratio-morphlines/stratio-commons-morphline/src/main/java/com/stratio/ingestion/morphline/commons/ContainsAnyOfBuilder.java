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

import com.typesafe.config.Config;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;

import java.util.*;

// @formatter:off

/**
 * The containsAnyOf is a command that succeeds if all field values of the given named fields contains any of the given values and fails otherwise. Multiple fields can be named, in which case a logical AND is applied to the results.
 * In following example succeeds if value of app field is "one", "two" or "three":
 * {
      {
        containsAnyOf {
          app : [one, two, three]
      }
   }
 */
// @formatter:on
public class ContainsAnyOfBuilder implements CommandBuilder {

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("containsAnyOf");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new ContainsAnyOf(this, config, parent, child, context);
    }

    private static final class ContainsAnyOf extends AbstractCommand {
        private final Set<Map.Entry<String, Object>> entrySet;

        protected ContainsAnyOf(CommandBuilder builder, Config config, Command parent, Command child,
                                MorphlineContext context) {
            super(builder, config, parent, child, context);

            this.entrySet = new Configs().getEntrySet(config);

        }

        @Override
        protected boolean doProcess(Record record) {
            boolean mark = false;
            Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                List list = record.get(entry.getKey());
                List values = (List) entry.getValue();
                for (int i = 0; i < list.size(); ++i) {
                    for (int j = 0; j < values.size(); ++j) {
                        if (list.get(i).equals(values.get(j))) {
                            mark = true;
                            break;
                        }
                    }
                }
                if (!mark) {
                    return false;
                }
            }

            return super.doProcess(record);
        }
    }
}
