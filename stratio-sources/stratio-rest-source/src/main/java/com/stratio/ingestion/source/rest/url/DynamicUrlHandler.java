/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.ingestion.source.rest.url;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.flume.Context;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.stratio.ingestion.source.rest.exception.RestSourceException;
import com.stratio.ingestion.source.rest.url.filter.CheckpointFilterHandler;
import com.stratio.ingestion.source.rest.url.filter.type.CheckpointType;

/**
 * Created by eambrosio on 5/02/15.
 */
public class DynamicUrlHandler implements UrlHandler {
    private static final String CHECKPOINT_CONF = "checkpointConfiguration";
    private static final String PARAM_MAPPER = "urlParamMapper";
    public static final String URL = "url";

    private CheckpointFilterHandler checkpointFilterHandler;
    private Map<String, String> checkpointFilterContext;

    @Override public String buildUrl(Map<String, String> properties) {
        String url = properties.get(URL);

        if (properties.get(PARAM_MAPPER) != null && !properties.get(PARAM_MAPPER).trim().equals("")) {
            Map<String, String> checkpoint = getCheckPoint(checkpointFilterContext);

            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(properties.get(PARAM_MAPPER)).get("params");
                Iterator<JsonNode> iterator = jsonNode.getElements();
                while (iterator.hasNext()) {
                    JsonNode currentNode = iterator.next();
                    if (currentNode.get("name") != null && (!(currentNode.get("name").asText().trim().equals("")))) {
                        url = replaceParameter(url, currentNode, checkpoint);
                    }
                }
            } catch (JsonProcessingException e) {
                throw new RestSourceException("Error during mapping url params", e);
            } catch (IOException e) {
                throw new RestSourceException("Error during mapping url params", e);
            }  catch (Exception e) {
                throw new RestSourceException("Error on param replacement", e);
            }
        }

        if (url.indexOf("${'") > -1) {
            //There is any not replaced param
            throw new RestSourceException("Some params in the REST were not satisfied");
        }

        return url;
    }

    /**
     * Update filter parameters with the last event data received
     *
     * @param filterParameters
     */
    @Override public void updateFilterParameters(String filterParameters) {
        checkpointFilterHandler.updateCheckpoint(filterParameters);

    }

    /**
     * Set up url handler dependencies
     *
     * @param context
     */
    @Override public void configure(Context context) {
        checkpointFilterContext = loadCheckpointContext(context);
        checkpointFilterHandler = getCheckPointHandler(checkpointFilterContext);
    }

    /**
     * Sets a param value into its placeholder
     *
     * @param url          Current REST URL
     * @param currentParam paramMapper specification
     * @param checkpoint   current checkpoint
     * @return
     */
    private String replaceParameter(String url, JsonNode currentParam, Map<String, String> checkpoint)
            throws Exception {
        String placeHolder = "${" + currentParam.get("name").asText() + "}";
        if (!url.contains(placeHolder)) {
            return url;
        }

        if (checkpoint != null && checkpoint.containsKey(currentParam.get("name").asText())) {
            url = url.replace(placeHolder, checkpoint.get(currentParam.get("name").asText()));
            return url;
        }

        if (currentParam.get("default") != null && !currentParam.get("default").asText().trim().equals("")) {
            url = url.replace(placeHolder, currentParam.get("default").asText());
        } else {
            throw new Exception("Can't replace the " + currentParam.get("name") + " parameter");
        }

        return url;
    }

    /**
     * Returns the checkpoint parameter as a Map. the key is the param name
     *
     * @return
     * @throws Exception
     */
    private CheckpointFilterHandler getCheckPointHandler(Map<String, String> filterContext) {
        CheckpointFilterHandler filterHandler = null;

        if (filterContext != null) {
            Constructor constructor;
            try {
                constructor = Class.forName(filterContext.get("handler")).getConstructor(CheckpointType.class,
                        Map.class);
                filterHandler = (CheckpointFilterHandler) constructor.newInstance(Class.forName(filterContext.get
                        ("checkpointType")).newInstance(), filterContext);
            } catch (Exception e) {
                throw new RestSourceException("An error occurred during CheckpointFilterHandler instantiation", e);
            }

        }
        return filterHandler;
    }

    /**
     * Returns the checkpoint parameter as a Map. the key is the param name
     *
     * @return
     * @throws Exception
     */
    private Map<String, String> getCheckPoint(Map<String, String> filterContext) {
        final String lastCheckpoint = checkpointFilterHandler.getLastCheckpoint(filterContext);
        Map<String, String> checkPoint = new HashMap<String, String>();
        checkPoint.put(filterContext.get("field"), lastCheckpoint);
        return checkPoint;
    }

    /**
     * Loads the context configured in the parameter 'checkpointConfiguration'
     *
     * @param context
     * @return
     * @throws Exception
     */
    private Map<String, String> loadCheckpointContext(Context context) {
        Map<String, String> checkpointContext = null;
        JsonNode jsonNode;

        if (context.getString(CHECKPOINT_CONF) != null && !context.getString(CHECKPOINT_CONF).trim().equals("")) {
            try {
                File checkpointFile = new File(context.getString(CHECKPOINT_CONF));
                if (checkpointFile.exists()) {
                    checkpointContext = new HashMap<String, String>();
                    ObjectMapper mapper = new ObjectMapper();
                    jsonNode = mapper.readTree(checkpointFile);
                    checkpointContext.put("handler", jsonNode.findValue("handler").asText());
                    checkpointContext.put("field", jsonNode.findValue("field").asText());
                    checkpointContext.put("mongoUri", jsonNode.findValue("mongoUri").asText());
                    checkpointContext.put("checkpointType", jsonNode.findValue("type").asText());
                    checkpointContext.put("format", jsonNode.findValue("format").asText());
                } else {
                    throw new RestSourceException("The checkpoint configuration file doesn't exist");
                }
            } catch (Exception e) {
                throw new RestSourceException("An error ocurred while json parsing. Verify checkpointConfiguration", e);
            }
        }
        return checkpointContext;
    }
}
