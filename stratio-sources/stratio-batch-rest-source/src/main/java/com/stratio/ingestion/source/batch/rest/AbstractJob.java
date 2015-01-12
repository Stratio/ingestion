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
package com.stratio.ingestion.source.batch.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.stratio.ingestion.source.batch.rest.checkpoint.CheckpointType;

/**
 * Created by eambrosio on 8/01/15.
 */
public abstract class AbstractJob {

    protected static final String CONF_CHECKPOINT_VALUE = "checkpointValue";
    protected static final String CONF_DATE_PATTERN_FIELD = "datePattern";
    protected static final String CONF_CHECKPOINT_ENABLED = "checkpointEnabled";
    protected static final String CONF_CHECKPOINT_TYPE = "checkpointType";
    protected static final String CONF_CHECKPOINT_FIELD = "checkpointField";

    public boolean isValid(JsonNode element, String checkpointTypeValue, String lastCheckpoint,
            String currentCheckpointField, Map<String, String> properties) {
        boolean isValid = Boolean.TRUE;
        CheckpointType checkpointType;

        try {
            checkpointType = (CheckpointType) Class.forName(checkpointTypeValue)
                    .newInstance();
            isValid = checkpointType
                    .isValid(lastCheckpoint, getCurrentCheckpoint(element, currentCheckpointField), properties);

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return isValid;
    }

    public boolean isCheckpointEnabled(String checkpointEnabled) {
        boolean isCheck = Boolean.FALSE;
        if (!StringUtils.isEmpty(checkpointEnabled)) {
            isCheck = Boolean.valueOf(checkpointEnabled);
        }
        return isCheck;
    }

    public void updateLastEventProperty(JsonNode element, Map<String, String> properties, String
            currentCheckpointField) {
        properties.put(CONF_CHECKPOINT_VALUE, getCurrentCheckpoint(element, currentCheckpointField));

    }

    public String getCurrentCheckpoint(JsonNode element, String currentCheckpointField) {
        return element.get(currentCheckpointField).asText();
    }

    public ObjectNode populateCheckpoint(ObjectMapper mapper, JsonNode element) {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put(CONF_CHECKPOINT_FIELD, element.get(CONF_CHECKPOINT_FIELD));
        objectNode.put("checkpoint", "true");
        return objectNode;
    }

    /**
     * Convert Multivalued Headers to Plain Map Headers accepted by Flume Event.
     *
     * @param map multivaluedMap.
     * @return plain Map.
     */
    protected Map<String, String> responseToHeaders(MultivaluedMap<String, String> map) {
        Map<String, String> newMap = new HashMap<String, String>();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            newMap.put(entry.getKey(), multiValueHeaderToString(entry.getValue()));
        }
        return newMap;
    }

    /**
     * Convert a multivalue header to String.
     *
     * @param list
     * @return
     * @see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2">w3.org</a>
     */
    private String multiValueHeaderToString(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); ++i) {
            sb.append(list.get(i));
            if (i != list.size() - 1) {
                sb.append(", ");
            }
        }

        return sb.toString();
    }
}
