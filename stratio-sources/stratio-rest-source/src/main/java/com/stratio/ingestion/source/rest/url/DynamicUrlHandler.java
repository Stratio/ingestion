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
package com.stratio.ingestion.source.rest.url;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.ingestion.source.rest.exception.RestSourceException;
import com.stratio.ingestion.source.rest.url.filter.FilterHandler;

/**
 * Created by eambrosio on 5/02/15.
 */
public class DynamicUrlHandler implements UrlHandler {
    private static final String PARAM_MAPPER = "urlParamMapper";
    private static final String URL = "url";
    public static final String URL_CONF = "urlJson";

    private FilterHandler filterHandler;
    private Map<String, String> urlContext;

    @Override public String buildUrl(Map<String, String> properties) {
        String url = properties.get(URL);

        if (StringUtils.isNotBlank(urlContext.get(PARAM_MAPPER))) {
//            Map<String, String> filter = filterHandler.getLastFilter(properties);
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(urlContext.get(PARAM_MAPPER)).get("params");
                Iterator<JsonNode> iterator = jsonNode.elements();
                while (iterator.hasNext()) {
                    JsonNode currentNode = iterator.next();
                    if (currentNode.get("name") != null && (!(currentNode.get("name").asText().trim().equals("")))) {
                        url = replaceParameter(url, currentNode, properties);
                    }
                }
            } catch (JsonProcessingException e) {
                throw new RestSourceException("Error during mapping url params", e);
            } catch (IOException e) {
                throw new RestSourceException("Error during mapping url params", e);
            } catch (Exception e) {
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
        filterHandler.updateFilter(filterParameters);

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
            url = url.replace("+", "%2B");
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
     * Set up url handler dependencies
     *
     * @param context
     */
    @Override public void configure(Context context) {
        urlContext = loadUrlContext(context);
        filterHandler = getFilterHandler(urlContext);
    }

    private Map<String, String> loadUrlContext(Context context) {
        Map<String, String> urlContext = new HashMap<String, String>();
        JsonNode jsonNode = loadConfiguration(context.getString(URL_CONF));
        urlContext.put("filterHandler", jsonNode.findValue("filterHandler").asText());
        urlContext.put("filterConfiguration", jsonNode.findValue("filterConfiguration").asText());
        JsonNode listParams = jsonNode.path("urlParamMapper");
        urlContext.put("urlParamMapper", listParams.toString());
        return urlContext;
    }

    private JsonNode loadConfiguration(String jsonFile) {
        JsonNode jsonNode = null;
        if (StringUtils.isNotBlank(jsonFile)) {
            try {
                File filterFile = new File(jsonFile);
                if (filterFile.exists()) {
                    ObjectMapper mapper = new ObjectMapper();
                    jsonNode = mapper.readTree(filterFile);
                } else {
                    throw new RestSourceException("The configuration file doesn't exist");
                }
            } catch (Exception e) {
                throw new RestSourceException("An error ocurred while json parsing. Verify configuration  file", e);
            }
        }
        return jsonNode;
    }

    /**
     * Returns the checkpoint parameter as a Map. the key is the param name
     *
     * @param context
     * @return
     * @throws Exception
     */
    private FilterHandler getFilterHandler(Map<String, String> context) {
        FilterHandler filterHandler = null;
        if (context != null) {
            try {
                filterHandler = (FilterHandler) Class.forName(context.get("filterHandler")).newInstance();
                filterHandler.configure(context);
            } catch (Exception e) {
                throw new RestSourceException("An error occurred during FilterHandler instantiation", e);
            }
        }
        return filterHandler;
    }

}
