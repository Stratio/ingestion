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
package com.stratio.ingestion.morphline.detector;

import com.stratio.ingestion.detector.source.http.handler.DetectorJsonHandler;
import com.typesafe.config.Config;
import org.apache.log4j.Logger;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import java.util.Collection;
import java.util.Collections;

/**
 * Parsing command for detector anchor morphlines.
 */
public class DetectorTramasBuilder implements CommandBuilder {

    private final static Logger LOG = Logger.getLogger(DetectorTramasBuilder.class);

    private final static String COMMAND_NAME = "parseDetectorTramasLine";

    private final static String ASSET = "asset";
    private final static String LAT = "lat";
    private final static String LON = "lon";
    private final static String V = "v";
    private final static String RPM = "rpm";
    private final static String ODOMETER = "odometer";
    private final static String IGNITION = "ignition";

    private static DetectorJsonHandler jsonHandler = new DetectorJsonHandler();

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList(COMMAND_NAME);
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new DetectorAnchorFrame(this, config, parent, child, context);
    }

    private static final class DetectorAnchorFrame extends AbstractCommand {

        public DetectorAnchorFrame(CommandBuilder builder, Config config, Command parent, Command child, final MorphlineContext context) {
            super(builder, config, parent, child, context);
        }

        @Override
        protected boolean doProcess(Record record) {
            String line = (String)(record.get("message").get(0));
            String [] fields = line.split("\\s+");
            try {
                record.put(ASSET, fields[0]);
                record.put(LAT, fields[1]);
                record.put(LON, fields[2]);
                record.put(V, fields[3]);
                record.put(RPM, fields[4]);
                record.put(ODOMETER, fields[5]);
                record.put(IGNITION, fields[6]);
                record.removeAll("message");
            } catch (Exception e) {
                LOG.warn("Error while parsing event: " + line, e);
                return false;
            }
            return super.doProcess(record);
        }
    }

}
