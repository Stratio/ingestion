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
package com.stratio.ingestion.morphline.geolocateairports;

import com.typesafe.config.Config;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import java.io.*;
import java.util.*;

/**
 * CommandBuilder for geolocating airport with their coordinates.
 */
public class AirportLatLonBuilder implements CommandBuilder {

    private final static Logger LOG = Logger.getLogger(AirportLatLonBuilder.class);

    private final static String COMMAND_NAME = "geolocateAirportCode";

    private static final Map<String, String> AIRPORT_COORDS = new HashMap<String, String>();

    static {
        InputStream is = null;
        BufferedReader br = null;
        try {
            is = AirportLatLonBuilder.class.getClassLoader().getResourceAsStream("airports.dat");
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));;
            CSVParser parser = new CSVParser(br, CSVFormat.DEFAULT.withDelimiter(',').withQuote('"'));
            List<CSVRecord> records = parser.getRecords();
            for(int i = 0; i < records.size(); i++) {
                CSVRecord record = records.get(i);
                AIRPORT_COORDS.put(record.get(4), record.get(6) + "," + record.get(7));
            }
        } catch(IOException e) {
            System.out.println(e);
        } finally {
            if(is != null) {
                try {
                    is.close();
                } catch (Exception e) {

                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {

                }
            }
        }
        LOG.info("Loaded [" + AIRPORT_COORDS.size() + "] airports coordinates.");
    }

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList(COMMAND_NAME);
    }

    @Override
    public Command build(Config config, Command parend, Command child, MorphlineContext context) {
        return new SGeolocatedAirport(this, config, parend, child, context);
    }

    private static final class SGeolocatedAirport extends AbstractCommand {

        private final static String DEFAULT_ORIGIN_FIELD_NAME = "Origin";
        private final static String DEFAULT_DESTINATION_FIELD_NAME = "Dest";

        private final static String ORIGIN_FIELD = "origin";
        private final static String DESTINATION_FIELD = "dest";

        private final static String DEFAULT_ORIGIN_LAT_LON_FIELD_NAME = "originLatLon";
        private final static String DEFAULT_DESTINATION_LAT_LON_FIELD_NAME = "destLatLon";

        private final static String ORIGIN_LAT_LON = "originLatLon";
        private final static String DESTINATION_LAT_LON = "destLatLon";

        private String originFieldName;
        private String originLatLonFieldName;
        private String destinationFieldName;
        private String destinationLatLonFieldName;

        public SGeolocatedAirport(CommandBuilder builder, Config config, Command parent, Command child, final MorphlineContext context) {
            super(builder, config, parent, child, context);
            this.originFieldName = getConfigs().getString(config, ORIGIN_FIELD, DEFAULT_ORIGIN_FIELD_NAME);
            this.destinationFieldName = getConfigs().getString(config, DESTINATION_FIELD, DEFAULT_DESTINATION_FIELD_NAME);
            this.originLatLonFieldName = getConfigs().getString(config, ORIGIN_LAT_LON, DEFAULT_ORIGIN_LAT_LON_FIELD_NAME);
            this.destinationLatLonFieldName = getConfigs().getString(config, DESTINATION_LAT_LON, DEFAULT_DESTINATION_LAT_LON_FIELD_NAME);
            validateArguments();
        }

        @Override
        protected boolean doProcess(Record record) {
            Object origin = record.get(originFieldName).get(0);
            Object destination = record.get(destinationFieldName).get(0);
            LOG.info("Processing record with origin airport [" + origin + "] and destination airport [" + destination + "].");
            record.put(originLatLonFieldName, AIRPORT_COORDS.get(origin));
            record.put(destinationLatLonFieldName, AIRPORT_COORDS.get(destination));
            LOG.info("Origin airport coordinates: [" + record.get(originLatLonFieldName) + "]");
            LOG.info("Destination airport coordinates: [" + record.get(destinationLatLonFieldName) + "]");
            return super.doProcess(record);
        }

    }

}
