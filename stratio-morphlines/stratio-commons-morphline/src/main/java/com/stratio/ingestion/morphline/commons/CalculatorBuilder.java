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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.typesafe.config.Config;

//@formatter:off
/**
* The calculator command makes computations between fields or literals and save result into a new field as BigDecimal.
 * <ul>
 * <li><em>leftOperand</em>: Field or literal which will be left operand.</li>
 * <li><em>rightOperand</em>: Field or literal which will be right operand.</a>.
 * <li><em>operator</em>: Operator. Possible values: +,-,*,/,%,sqrt,^</li>
 * <li><em>output</em>: Output field. Default: output</li>
 * </ul>
 * <code>
 * Example: 
 * { 
 *     {
        calculator {
          leftOperand : metric
          operator : "/"
          rightOperand : 2
          output : "result"
        }
      }
 * }
 * </code>
* 
*/
//@formatter:on
public class CalculatorBuilder implements CommandBuilder {

    private static final String CONF_OPERAND1 = "leftOperand";
    private static final String CONF_OPERAND2 = "rightOperand";
    private static final String CONF_OUTPUT_FIELD = "output";
    private static final String CONF_OPERATOR = "operator";

    private static final String DEFAULT_OUT_FIELD = "output";

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("calculator");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new Calculator(this, config, parent, child, context);
    }

    private static final class Calculator extends AbstractParser {
        private String operator;
        private String outField;

        private Object op1;
        private Object op2;

        protected Calculator(CommandBuilder builder, Config config, Command parent, Command child,
                MorphlineContext context) {
            super(builder, config, parent, child, context);

            String operand1 = getConfigs().getString(config, CONF_OPERAND1);
            String operand2 = getConfigs().getString(config, CONF_OPERAND2, null);
            operator = getConfigs().getString(config, CONF_OPERATOR);

            outField = getConfigs().getString(config, CONF_OUTPUT_FIELD, DEFAULT_OUT_FIELD);
            checkOperands(operand1, operand2);

        }

        @Override
        protected boolean doProcess(Record record, InputStream stream) throws IOException {
            Record outputRecord = record.copy();
            BigDecimal result = doComputation(outputRecord);
            outputRecord.put(outField, result);
            if (!getChild().process(outputRecord)) {
                return false;
            }
            return true;
        }

        private void checkOperands(String rawOp1, String rawOp2) {
            try {
                op1 = new BigDecimal(rawOp1);
            } catch (NumberFormatException ex) {
                op1 = rawOp1;
            }
            if (rawOp2 != null) {
                try {
                    op2 = new BigDecimal(rawOp2);
                } catch (NumberFormatException ex) {
                    op2 = rawOp2;
                }
            }
        }

        private BigDecimal doComputation(Record record) {
            BigDecimal opLeft = null, opRight = null, result = null;
            if (op1 instanceof String) {
                String field = String.valueOf(op1);
                if(field != null){
                    opLeft = new BigDecimal(String.valueOf(record.getFirstValue(field)));
                } else {
                    throw new RuntimeException("Inexistent field " + field);
                }
                
            } else if (op1 instanceof BigDecimal) {
                opLeft = (BigDecimal) op1;
            }

            if (op2 instanceof String) {
                String field = String.valueOf(op2);
                if(field != null){
                    opRight = new BigDecimal(String.valueOf(record.getFirstValue(field)));
                } else {
                    throw new RuntimeException("Inexistent field " + field);
                }
            } else if (op2 instanceof BigDecimal) {
                opRight = (BigDecimal) op2;
            }

            switch (String.valueOf(operator)) {
                case "+":
                    result = opLeft.add(opRight);
                    break;
                case "-":
                    result = opLeft.subtract(opRight);
                    break;
                case "*":
                    result = opLeft.multiply(opRight);
                    break;
                case "/":
                    result = opLeft.divide(opRight);
                    break;
                case "^":
                    result = opLeft.pow(opRight.intValue());
                    break;
                case "sqrt":
                    result = new BigDecimal(Math.sqrt(opLeft.doubleValue()));
                    break;
                case "%":
                    result = opLeft.remainder(opRight);
                    break;
                default:
                    throw new RuntimeException("Incorrect operator");
            }

            return result;
        }
    }

}
