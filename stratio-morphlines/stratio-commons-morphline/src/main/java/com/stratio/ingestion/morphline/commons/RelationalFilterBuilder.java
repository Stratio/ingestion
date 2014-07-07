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

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

//@formatter:off
/**
* <p>The relationalFilter command discard records that are not accomplish a relational condition. <p>
* <ul>
* <li><em>field</em>: Name of field which value will be compared.</li>
* <li><em>operator</em>: Operator. Possible values: <, <=, >, >=, ==, <>.</li>
* <li><em>reference</em>: Number to compare to.</li>
* <li><em>class</em>: Class to cast reference number.</li>
* </ul>
* <code>
* Example: 
* { 
*     relationalFilter {
          field : field1
          operator : >
          reference : 100.0
          class : java.lang.Double
        }
* }
* </code>
* 
*/
//@formatter:on
public class RelationalFilterBuilder  implements CommandBuilder {
    
    private static final String COMMAND_NAME = "relationalFilter";
    
    private static final String CONF_CLASS = "class";
    private static final String CONF_FIELD = "field";
    private static final String CONF_REFERENCE= "reference";
    private static final String CONF_OPERATOR = "operator";

    private static final String DEFAULT_CLASS = "java.lang.Integer";
    private static final String DEFAULT_OPERATOR = "+";

    @Override
    public Collection<String> getNames() {
        return Collections.singleton(COMMAND_NAME);
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new RelationalFilter(this, config, parent, child, context);
    }

    private static final class RelationalFilter extends AbstractCommand {

        private String classname;
        private String field;
        private String reference;
        private String operator;

        protected RelationalFilter(CommandBuilder builder, Config config, Command parent, Command child,
                MorphlineContext context) {
            super(builder, config, parent, child, context);
            
            classname = getConfigs().getString(config, CONF_CLASS, DEFAULT_CLASS);
            field = getConfigs().getString(config, CONF_FIELD);
            reference = getConfigs().getString(config, CONF_REFERENCE);
            operator = getConfigs().getString(config, CONF_OPERATOR, DEFAULT_OPERATOR);
            
        }

        @Override
        protected boolean doProcess(Record record) {
            Object current = record.getFirstValue(field);
            Object refObject = null;
            try {
                refObject = Class.forName(classname).getConstructor(String.class).newInstance(reference);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException | NoSuchMethodException | SecurityException
                    | ClassNotFoundException e) {
                e.printStackTrace();
            }
            
            boolean result = checkCondition(current, refObject);
            
            return (result) ? super.doProcess(record) : true;
        }
                
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private boolean checkCondition(Object current, Object refObject){
            boolean result = false;
            int comparationResult = compare(current, refObject);
            switch(operator){
                case ">":
                    result =  comparationResult > 0;
                    break;
                case ">=":
                    result = comparationResult >= 0;
                    break;
                case "<":
                    result = comparationResult < 0;
                    break;
                case "<=":
                    result = comparationResult <= 0;
                    break;
                case "==":
                    result = comparationResult == 0;
                    break;
                case "<>":
                    result = comparationResult != 0;
                    break;
            }
            
            return result;
        }
        
        private int compare(Object current, Object ref){
            return new Double(String.valueOf(current)).compareTo(new Double(String.valueOf(ref)));
        }
    }
}
