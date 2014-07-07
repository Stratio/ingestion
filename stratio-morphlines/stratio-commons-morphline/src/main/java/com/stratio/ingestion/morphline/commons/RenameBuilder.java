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

import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

//@formatter:off
/**
* The rename command simply renames fields in morphline records. It takes two parameters. 
* <ul>
*   <li><em>remove</em>: boolean that indicates if old entry must be removed after rename.</li>
*   <li><em>fields</em>: {@code com.typesafe.config.Config} where each entry indicates old name and new name.</li>
* </ul>
* Example:  <br/>
* <code>{ 
*     rename {
          remove : true
          fields {
            field1 : newfield1
            field2 : newfield2
          }
        }
* }</code>
* 
*/
//@formatter:on
public class RenameBuilder implements CommandBuilder {

    private static final String COMMAND_NAME = "rename";

    private static final String CONF_REMOVE = "remove";
    private static final String CONF_FIELDS = "fields";

    private static final Boolean DEFAULT_REMOVE = true;

    @Override
    public Collection<String> getNames() {
        return Collections.singleton(COMMAND_NAME);
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new Rename(this, config, parent, child, context);
    }

    private static final class Rename extends AbstractCommand {

        private Set<Entry<String, ConfigValue>> entries;
        private boolean remove;

        protected Rename(CommandBuilder builder, Config config, Command parent, Command child,
                MorphlineContext context) {
            super(builder, config, parent, child, context);

            remove = getConfigs().getBoolean(config, CONF_REMOVE, DEFAULT_REMOVE);
            Config paths = getConfigs().getConfig(config, CONF_FIELDS);
            entries = paths.entrySet();
        }

        @Override
        protected boolean doProcess(Record record) {
            Record outputRecord = record.copy();
            for (Entry<String, ConfigValue> entry : entries) {
                outputRecord.put(entry.getValue().render().replace("\"", ""),
                        outputRecord.get(entry.getKey()).get(0));
                if (remove) {
                    outputRecord.removeAll(entry.getKey());
                }
            }

            return super.doProcess(outputRecord);
        }
    }

}
