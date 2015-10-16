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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.stratio.ingestion.source.rest.exception.RestSourceException;
import com.stratio.ingestion.source.rest.url.filter.FilterHandler;

import junit.framework.TestCase;

/**
 * Created by miguelsegura on 7/10/15.
 */
public class DynamicUrlHandlerTest extends TestCase {

    private static final String PARAM_MAPPER = "urlParamMapper";
    private static final String URL = "url";
    private static final String URL_JSON = "src/test/resources/filterConfiguration.json";
    private static final String BAD_URL_JSON = "src/test/resources/badJson.json";
    private static final String EMPTY_URL_JSON = "src/test/resources/emptyFilter.json";
    DynamicUrlHandler dynamicUrlHandler = mock(DynamicUrlHandler.class);
    Map<String, String> properties = new HashMap();
    Map<String, String> urlContext = mock(HashMap.class);
    JsonNode jsonNode = mock(JsonNode.class);
    Context context = new Context();
    FilterHandler filterHandler = mock(FilterHandler.class);

    @Before
    public void setUp(){

        properties.put("url", "URL");
        properties.put("field", "date");
        properties.put("type", "com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType");
        properties.put("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        properties.put("mongoUri", "mongodb://socialLoginUser:temporal@180.205.132.228:50017,180.205.132.229:50017,"
                + "180.205.132.230:50017/socialLogin.checkpoints?replicaset=socialLogin&ssl=true");
        when(filterHandler.getLastFilter(properties)).thenReturn(properties);

    }

    @Test
    public void testMethod() throws Exception {
        try{
            context.put("urlJson", URL_JSON);

            dynamicUrlHandler = new DynamicUrlHandler();
            dynamicUrlHandler.configure(context);

        }catch(RestSourceException e){
            assertNotNull(e.getMessage());
            assertEquals(e.getMessage(), "An error occurred during FilterHandler instantiation");
        }
        finally{
            dynamicUrlHandler.buildUrl(properties);
        }
    }

    @Test(expected=RestSourceException.class)
    public void testJson() throws Exception {
        context.put("urlJson", URL_JSON);
        dynamicUrlHandler = new DynamicUrlHandler();
        try{
            dynamicUrlHandler.configure(context);
        }catch(RestSourceException e){
            assertNotNull(e.getMessage());
            assertEquals(e.getMessage(), "An error occurred during FilterHandler instantiation");
        }
    }

    @Test
    public void testBadJson() {
        context.put("urlJson", BAD_URL_JSON);
        dynamicUrlHandler = new DynamicUrlHandler();
        try{
            dynamicUrlHandler.configure(context);
        }catch(RestSourceException e){
            assertNotNull(e.getMessage());
            assertEquals(e.getMessage(), "An error ocurred while json parsing. Verify configuration  file");
        }
    }

    @Test
    public void testEmptyJson() throws Exception {
        context.put("urlJson", EMPTY_URL_JSON);
        dynamicUrlHandler = new DynamicUrlHandler();
        try{
            dynamicUrlHandler.configure(context);
        }catch(RestSourceException e){
            assertNotNull(e.getMessage());
            assertEquals(e.getMessage(), "An error ocurred while json parsing. Verify configuration  file");
        }
    }

}