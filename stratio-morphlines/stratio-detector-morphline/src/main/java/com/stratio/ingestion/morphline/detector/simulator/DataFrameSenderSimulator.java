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
package com.stratio.ingestion.morphline.detector.simulator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by mariomgal on 25/5/15.
 */
public class DataFrameSenderSimulator {

    private final static int NUM_THREADS = 1;
    private final static int NUM_REQUESTS = 100;
    private final static int REQUEST_INTERVAL_MS = 10;

    private final static Logger LOG = Logger.getLogger(DataFrameSenderSimulator.class);

    private static final Map<String, Map<String, String>> MASTER_DATA = new HashMap<String, Map<String, String>>();
    private static final List<String> MASTER_DATA_KEYS = new ArrayList<String>();

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

    static {
        InputStream is = null;
        BufferedReader br = null;
        try {
            is = DataFrameSenderSimulator.class.getClassLoader().getResourceAsStream("DatosMaestros.csv");
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
                MASTER_DATA_KEYS.add(record.get(0).trim());
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

    private static String createJsonEntity() {
        Random random = new Random();
        //String asset = MASTER_DATA_KEYS.get(random.nextInt(MASTER_DATA_KEYS.size()));
        long id = random.nextLong();

        String [] assets = {"1234567", "", "1919191", "2000000"};
        String asset = assets[random.nextInt(assets.length)];


        String json = "[{\n" +
                "\n" +
                "    \"meta\": {\n" +
                "\n" +
                "        \"account\": \"detector\",\n" +
                "\n" +
                "        \"event\": \"track\"\n" +
                "\n" +
                "    },\n" +
                "\n" +
                "    \"payload\": {\n" +
                "\n" +
                "        \"id\": " + id + ",\n" +
                "\n" +
                "        \"connection_id\": 709075993670713474,\n" +
                "\n" +
                "        \"id_str\": \"709078216395997305\",\n" +
                "\n" +
                "        \"connection_id_str\": \"709075993670713474\",\n" +
                "\n" +
                "        \"index\": 976,\n" +
                "\n" +
                "        \"asset\": \"" + asset + "\",\n" +
                "\n" +
                "        \"recorded_at\": \"2015-05-11T16:23:32Z\",\n" +
                "\n" +
                "        \"recorded_at_ms\": \"2015-05-11T16:23:32.000Z\",\n" +
                "\n" +
                "        \"received_at\": \"2015-05-11T16:23:25Z\",\n" +
                "\n" +
                "        \"loc\": [\n" +
                "\n" +
                "            -0.48549,\n" +
                "\n" +
                "            38.35483\n" +
                "\n" +
                "        ],\n" +
                "\n" +
                "        \"fields\": {\n" +
                "\n" +
                "            \"DET_VOLT\": {\n" +
                "\n" +
                "                \"b64_value\": \"AAA1mw==\"\n" +
                "\n" +
                "            },\n" +
                "\n" +
                "            \"DET_ODOMETER\": {\n" +
                "\n" +
                "                \"b64_value\": \"AAAGPQ==\"\n" +
                "\n" +
                "            },\n" +
                "\n" +
                "            \"DET_ODOMETER_FULL\": {\n" +
                "\n" +
                "                \"b64_value\": \"AC1inA==\"\n" +
                "\n" +
                "            },\n" +
                "\n" +
                "            \"DET_ST_VEL\": {\n" +
                "\n" +
                "                \"b64_value\": \"MTQzMTM2MTQxMiw1MCw1Miw1NSw1Myw1NCw0OSw0OSw0OCw=\"\n" +
                "\n" +
                "            },\n" +
                "\n" +
                "            \"DET_ST_RPM\": {\n" +
                "\n" +
                "                \"b64_value\": \"MTQzMTM2MTQxMiwyMjE2LDIzMzcsMjQ1OSwyMzc2LDIzNzQsMjE1OCwyMTc5LDIwOTks\"\n" +
                "\n" +
                "            },\n" +
                "\n" +
                "            \"DET_TEMP\": {\n" +
                "\n" +
                "                \"b64_value\": \"AAAAYQ==\"\n" +
                "\n" +
                "            },\n" +
                "\n" +
                "            \"DET_ST_LOAD\": {\n" +
                "\n" +
                "                \"b64_value\": \"MTQzMTM2MTQxMiwyNyw1Miw1LDMxLDgsMTIsMTYsOSw=\"\n" +
                "\n" +
                "            },\n" +
                "\n" +
                "            \"GPS_DIR\": {\n" +
                "\n" +
                "                \"b64_value\": \"AACMHQ==\"\n" +
                "\n" +
                "            }\n" +
                "\n" +
                "        }\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "}]";
        return json;
    }

    private static class SenderThread implements Runnable {

        private String threadId;

        public SenderThread(int id) {
            threadId = "thread-" + id;
        }

        public void run() {
            HttpClient client = new DefaultHttpClient();
            HttpPost post = new HttpPost("http://localhost:8088");
            for (int i = 0; i < NUM_REQUESTS; i++) {
                try {
                    post.setEntity(new StringEntity(createJsonEntity(), ContentType.APPLICATION_JSON));
                    HttpResponse response = client.execute(post);
                    EntityUtils.consumeQuietly(response.getEntity());
                    System.out.println(threadId + ":" + response.getStatusLine().toString());
                    Thread.sleep(REQUEST_INTERVAL_MS);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(threadId + ": Error while sending POST request.");
                }
            }
        }
    }

    public static void main(String [] args) {

        for (int i = 0; i < NUM_THREADS; i++) {
            new Thread(new SenderThread(i)).start();
        }

    }


}
