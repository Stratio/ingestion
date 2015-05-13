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
package com.stratio.ingestion.detector.source.http.handler;

import org.apache.commons.codec.binary.Base64;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.nio.charset.UnsupportedCharsetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Handler for parsing Grupo Detector JSON received frames.
 */
public class DetectorJsonHandler implements HTTPSourceHandler, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(DetectorJsonHandler.class);

    private static final String PAYLOAD = "payload";
    private static final String ID = "id";
    private static final String CONNECTION_ID = "connection_id";
    private static final String INDEX = "index";
    private static final String ASSET = "asset";
    private static final String RECORDED_AT = "recorded_at";
    private static final String RECORDED_AT_MS = "recorded_at_ms";
    private static final String RECEIVED_AT = "received_at";
    private static final String LOC = "loc";
    private static final String LAT = "lat";
    private static final String LON = "lon";
    private static final String FIELDS = "fields";
    private static final String B64_VALUE = "b64_value";

    // Fields with value
    private static final String DET_ST_RPM = "DET_ST_RPM";
    private static final String DET_ST_SCORE_EMISIONES = "DET_ST_SCORE_EMISIONES";
    private static final String DET_ALARMA = "DET_ALARMA";
    private static final String DET_ODOMETER_FULL = "DET_ODOMETER_FULL";
    private static final String DET_IGNITION = "DET_IGNITION";
    private static final String DET_VOLT = "DET_VOLT";
    private static final String DET_ST_VEL = "DET_ST_VEL";
    private static final String DET_ST_CONSUMO = "DET_ST_CONSUMO";
    private static final String DET_ST_EMISIONES = "DET_ST_EMISIONES";
    private static final String DET_ST_SCORE_CONSUMO = "DET_ST_SCORE_CONSUMO";

    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat(DATE_FORMAT);
    private static final String DATE_FORMAT_MS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final SimpleDateFormat DATE_FORMATTER_MS = new SimpleDateFormat(DATE_FORMAT_MS);

    @Override public void configure(Context context) {

    }

    public List<Event> getEvents(final HttpServletRequest request) throws Exception {
        BufferedReader reader = request.getReader();
        String charset = request.getCharacterEncoding();
        if(charset == null) {
            LOG.debug("Charset is null, default charset of UTF-8 will be used.");
            charset = "UTF-8";
        } else if(!charset.equalsIgnoreCase("utf-8") && !charset.equalsIgnoreCase("utf-16") && !charset.equalsIgnoreCase("utf-32")) {
            LOG.error("Unsupported character set in request {}. JSON handler supports UTF-8, UTF-16 and UTF-32 only.", charset);
            throw new UnsupportedCharsetException("JSON handler supports UTF-8, UTF-16 and UTF-32 only.");
        }
        List<Event> events = new ArrayList<Event>(0);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(reader);
        if(jsonNode.isObject()) {
            events.add(buildDetectorEvent(jsonNode));
        } if(jsonNode.isArray()) {
            final Iterator<JsonNode> elements = jsonNode.getElements();
            JsonNode element;
            while (elements.hasNext()) {
                element = elements.next();
                events.add(buildDetectorEvent(element));
            }
        }
        return events;
    }

    private  Event buildDetectorEvent(JsonNode node) throws ParseException {
        Event event = new SimpleEvent();
        Map<String, String> headers = new HashMap<String, String>();
        JsonNode payload = node.get(PAYLOAD);
        JsonNode fieldsNode = payload.get(FIELDS);
        final Iterator<Map.Entry<String, JsonNode>> fields = fieldsNode.getFields();
        Map.Entry<String, JsonNode> field;
        headers.putAll(buildHeadersFromFrame(payload));
        while(fields.hasNext()){
            field = fields.next();
            try {
                headers.putAll(buildHeadersFromField(field.getKey(), field.getValue().get(B64_VALUE).asText()));
            } catch(Exception e) {
                LOG.warn("Error while building event from field [" + field.getKey() + ", " + field.getValue() + "]", e);
            }
        }
        event.setHeaders(headers);
        return event;
    }

    private Map<String, String> buildHeadersFromFrame(JsonNode payload) throws ParseException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put(ID, payload.get(ID).asText());
        headers.put(CONNECTION_ID, payload.get(CONNECTION_ID).asText());
        headers.put(INDEX, payload.get(INDEX).asText());
        headers.put(ASSET, payload.get(ASSET).asText());
        headers.put(RECORDED_AT, Long.toString(DATE_FORMATTER.parse(payload.get(RECORDED_AT).asText()).getTime()));
        headers.put(RECORDED_AT_MS, Long.toString(DATE_FORMATTER_MS.parse(payload.get(RECORDED_AT_MS).asText()).getTime()));
        headers.put(RECEIVED_AT, Long.toString(DATE_FORMATTER.parse(payload.get(RECEIVED_AT).asText()).getTime()));
        headers.put(LAT, payload.get(LOC).get(0).asText());
        headers.put(LON, payload.get(LOC).get(1).asText());
        return headers;
    }

    private Map<String, String> buildHeadersFromField(String fieldName, String base64FieldValue) throws Exception {
        Map<String, String> headers = new HashMap<String, String>();
        byte[] decodedValue = Base64.decodeBase64(base64FieldValue);
        if(DET_ST_RPM.equals(fieldName)) {
            headers = buildStRpmEvent(decodedValue);
        } else if(DET_ST_SCORE_EMISIONES.equals(fieldName)) {
            headers = buildStScoreEmissions(decodedValue);
        } else if(DET_ALARMA.equals(fieldName)) {
            headers = buildAlarm(decodedValue);
        } else if(DET_ODOMETER_FULL.equals(fieldName)) {
            headers = buildOdometerFull(decodedValue);
        } else if(DET_IGNITION.equals(fieldName)) {
            headers = buildIgnition(decodedValue);
        } else if(DET_VOLT.equals(fieldName)) {
            headers = buildVolt(decodedValue);
        } else if(DET_ST_VEL.equals(fieldName)) {
            headers = buildStVel(decodedValue);
        } else if(DET_ST_CONSUMO.equals(fieldName)) {
            headers = buildStConsumption(decodedValue);
        } else if(DET_ST_EMISIONES.equals(fieldName)) {
            headers = buildStEmissions(decodedValue);
        } else if(DET_ST_SCORE_CONSUMO.equals(fieldName)) {
            headers = buildStScoreConsumption(decodedValue);
        }
        return headers;
    }

    private Map<String, String> buildStRpmEvent(byte[] decodedBase64Value) throws Exception {
        String decodedString = new String(decodedBase64Value, "UTF-8");
        Map<String, String> headers = new HashMap<String, String>();
        //Timestamp EPOCH ms, RPM,RPM,RPM,RPM,RPM,RPM,RPM,
        String [] values = decodedString.split(",");
        headers.put("rpm_event_timestamp", values[0].trim());
        for (int i = 1; i < values.length; i++) {
            headers.put("rpm_event_" + (i-1), values[i].trim());
        }
        return headers;
    }

    private Map<String, String> buildStScoreEmissions(byte[] decodedBase64Value) throws Exception {
        String decodedString = new String(decodedBase64Value, "UTF-8");
        Map<String, String> headers = new HashMap<String, String>();
        // Timestamp EPOCH ms, Latitud, Longitud, Nota de consumo, Latitud, Longitud, Nota de consumo, Latitud, Longitud, Nota de consumo
        String [] values = decodedString.split(",");
        headers.put("score_emissions_timestamp", values[0].trim());
        headers.put("score_emissions_lat_0", values[1].trim());
        headers.put("score_emissions_lon_0", values[2].trim());
        headers.put("score_emissions_consumption_0", values[3].trim());
        headers.put("score_emissions_lat_1", values[4].trim());
        headers.put("score_emissions_lon_1", values[5].trim());
        headers.put("score_emissions_consumption_1", values[6].trim());
        return headers;
    }

    private Map<String, String> buildAlarm(byte[] decodedBase64Value) throws Exception {
        String decodedString = new String(decodedBase64Value, "UTF-8");
        Map<String, String> headers = new HashMap<String, String>();
        // alarma,0,código de alarma, IMEI, Timestamp EPOCH ms, latitud, longiud, número de satélites vistos,velocidad,rumbo,modemCSQ,cellid,lac,Ignicion,Tension de batería, DETL
        String [] values = decodedString.split(",");
        headers.put("alarm_timestamp", values[0].trim());
        headers.put("alarm_code", values[2].trim());
        headers.put("alarm_lat", values[3].trim());
        headers.put("alarm_lon", values[4].trim());
        headers.put("alarm_sat_number", values[5].trim());
        headers.put("alarm_speed", values[6].trim());
        headers.put("alarm_direction", values[7].trim());
        headers.put("alarm_modem_csq", values[8].trim());
        headers.put("alarm_cell_id", values[9].trim());
        headers.put("alarm_ignition", values[10].trim());
        headers.put("alarm_batt_tension", values[11].trim());
        headers.put("alarm_detl", values[12].trim());
        return headers;
    }

    private Map<String, String> buildOdometerFull(byte[] decodedBase64Value) throws Exception {
        String decodedString = new Integer(ByteBuffer.wrap(decodedBase64Value).getInt()).toString();
        Map<String, String> headers = new HashMap<String, String>();
        // Timestamp EPOCH ms, Odómetro total en metros
        headers.put("odometer", decodedString);
        return headers;
    }

    private Map<String, String> buildIgnition(byte[] decodedBase64Value) throws Exception {
        byte bool = decodedBase64Value[0];
        String result = bool == 1?"true":"false";
        Map<String, String> headers = new HashMap<String, String>();
        // True/false
        headers.put("ignition", result);
        return headers;
    }

    private Map<String, String> buildVolt(byte[] decodedBase64Value) throws Exception {
        String decodedString = new Integer(ByteBuffer.wrap(decodedBase64Value).getInt()).toString();
        Map<String, String> headers = new HashMap<String, String>();
        // Tensión de batería en mV
        headers.put("volt_batt_tension", decodedString.trim());
        return headers;
    }

    private Map<String, String> buildStVel(byte[] decodedBase64Value) throws Exception {
        String decodedString = new String(decodedBase64Value, "UTF-8");
        Map<String, String> headers = new HashMap<String, String>();
        String [] values = decodedString.split(",");
        // Timestamp EPOCH ms, VEL, VEL, VEL, VEL, VEL, VEL, VEL, (7-8 valores de velocidad en Km/h)
        headers.put("vel_timestamp", values[0].trim());
        for (int i = 1; i < values.length; i++) {
            headers.put("vel_"+(i-1), values[i].trim());
        }
        return headers;
    }

    private Map<String, String> buildStConsumption(byte[] decodedBase64Value) throws Exception {
        String decodedString = new String(decodedBase64Value, "UTF-8");
        Map<String, String> headers = new HashMap<String, String>();
        String [] values = decodedString.split(",");
        // Timestamp EPOCH ms, Latitud, Longitud, Consumo (l/100km), Latitud, Longitud, Consumo (l/100km), Latitud, Longitud, Consumo (l/100km)
        headers.put("consumption_timestamp", values[0].trim());
        headers.put("consumption_lat_0", values[1].trim());
        headers.put("consumption_lat_0", values[2].trim());
        headers.put("consumption_0", values[3].trim());
        headers.put("consumption_lat_1", values[4].trim());
        headers.put("consumption_lon_1", values[5].trim());
        headers.put("consumption_1", values[6].trim());
        headers.put("consumption_lat_2", values[7].trim());
        headers.put("consumption_lon_2", values[8].trim());
        headers.put("consumption_2", values[9].trim());
        return headers;
    }

    private Map<String, String> buildStEmissions(byte[] decodedBase64Value) throws Exception {
        String decodedString = new String(decodedBase64Value, "UTF-8");
        Map<String, String> headers = new HashMap<String, String>();
        String [] values = decodedString.split(",");
        // Timestamp EPOCH ms, Latitud, Longitud, Emisiones CO2 , Latitud, Longitud, Emisiones CO2, Latitud, Longitud, Emisiones CO2
        headers.put("emissions_timestamp", values[0].trim());
        headers.put("emissions_lat_0", values[1].trim());
        headers.put("emissions_lon_0", values[2].trim());
        headers.put("emissions_co2_0", values[3].trim());
        headers.put("emissions_lat_1", values[4].trim());
        headers.put("emissions_lon_1", values[5].trim());
        headers.put("emissions_co2_1", values[6].trim());
        headers.put("emissions_lat_2", values[7].trim());
        headers.put("emissions_lon_2", values[8].trim());
        headers.put("emissions_co2_2", values[9].trim());
        return headers;
    }

    private Map<String, String> buildStScoreConsumption(byte[] decodedBase64Value) throws Exception {
        String decodedString = new String(decodedBase64Value, "UTF-8");
        Map<String, String> headers = new HashMap<String, String>();
        String [] values = decodedString.split(",");
        // Timestamp EPOCH ms, Latitud, Longitud, Nota de consumo, Latitud, Longitud, Nota de consumo, Latitud, Longitud, Nota de consumo
        headers.put("score_consumption_timestamp", values[0].trim());
        headers.put("score_consumption_lat_0", values[1].trim());
        headers.put("score_consumption_lon_0", values[2].trim());
        headers.put("score_consumption_kpi_0", values[3].trim());
        headers.put("score_consumption_lat_1", values[4].trim());
        headers.put("score_consumption_lon_1", values[5].trim());
        headers.put("score_consumption_kpi_1", values[6].trim());
        headers.put("score_consumption_lat_2", values[7].trim());
        headers.put("score_consumption_lon_2", values[8].trim());
        headers.put("score_consumption_kpi_2", values[9].trim());
        return headers;
    }

}
