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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Builder for crossing Grupo Detector bulk-loaded JSON with master data loaded in memory.
 */
public class DetectorMasterDataBuilder implements CommandBuilder {

    private final static Logger LOG = Logger.getLogger(DetectorMasterDataBuilder.class);

    private final static String COMMAND_NAME = "addMasterData";

    private static final String ASSET = "asset";
    private static final String ID_GROUP = "id_group";
    private static final String ID_VEHICLE = "id_vehicle";
    private static final String SERIAL_NUM = "serial_num";
    private static final String PLATE = "plate";
    private static final String COMPANY_VEHICLE = "company_vehicle";
    private static final String COMPANY_VEHICLE_NAME = "company_vehicle_name";
    private static final String OU_VEHICLE = "ou_vehicle";
    private static final String OU_VEHICLE_NAME = "ou_vehicle_name";
    private static final String COMPANY_ROOT = "company_root";
    private static final String ID_MCUSTOMER = "id_mcustomer";
    private static final String DRIVER = "driver";

    private static final Map<String, Map<String, String>> MASTER_DATA = new HashMap<String, Map<String, String>>();

    static {
        InputStream is = null;
        BufferedReader br = null;
        try {
            is = DetectorMasterDataBuilder.class.getClassLoader().getResourceAsStream("DatosMaestros.csv");
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            CSVParser parser = new CSVParser(br, CSVFormat.DEFAULT.withDelimiter(',').withQuote('"'));
            List<CSVRecord> records = parser.getRecords();
            for (int i = 0; i < records.size(); i++) {
                CSVRecord record = records.get(i);
                LOG.warn(record);
                Map<String, String> row = new HashMap<String, String>();
                row.put(ID_GROUP, record.get(1).trim());
                row.put(ID_VEHICLE, record.get(2).trim());
                row.put(SERIAL_NUM, record.get(3).trim());
                row.put(PLATE, record.get(4).trim());
                row.put(COMPANY_VEHICLE, record.get(5).trim());
                row.put(COMPANY_VEHICLE_NAME, record.get(6).trim());
                row.put(OU_VEHICLE, record.get(7).trim());
                row.put(OU_VEHICLE_NAME, record.get(8).trim());
                row.put(COMPANY_ROOT, record.get(9).trim());
                row.put(ID_MCUSTOMER, record.get(10).trim());
                row.put(DRIVER, record.get(11).trim());
                MASTER_DATA.put(record.get(0).trim(), row);
            }
        } catch(IOException e) {
            LOG.error("Error while parsing master data CSV.", e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {
                    LOG.warn("Error while closing InputStream, maybe it was not properly closed.", e);
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    LOG.warn("Error while closing BufferedReader, maybe it was not properly closed.", e);
                }
            }
        }
        LOG.debug("Loaded [" + MASTER_DATA.size() + "] master data rows.");
    }

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList(COMMAND_NAME);
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new SRecordWithMasterData(this, config, parent, child, context);
    }

    private static final class SRecordWithMasterData extends AbstractCommand {

        public SRecordWithMasterData(CommandBuilder builder, Config config, Command parent, Command child, final MorphlineContext context) {
            super(builder, config, parent, child, context);
        }

        protected boolean doProcess(Record record) {
            try {
                String assetId = (String) (record.get(ASSET).get(0));
                Map<String, String> masterData = MASTER_DATA.get(assetId);
                if (masterData != null) {
                    for (Map.Entry<String, String> data : masterData.entrySet()) {
                        if (!data.getValue().isEmpty()) {
                            record.put(data.getKey(), data.getValue());
                        }
                    }
                }
                if (record.getFields().containsKey("message")) {
                    record.removeAll("message");
                }
                if (record.getFields().containsKey("file")) {
                    record.removeAll("file");
                }
                return super.doProcess(record);
            } catch (Exception e) {
                LOG.warn("Error while processing record [" + record + "]", e);
                return true; // Processing shall continue ignoring this error.
            }
        }

    }

}
