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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Created by miguelsegura on 7/10/15.
 */
public class DynamicUrlHandlerTest extends TestCase {

    private static final String PARAM_MAPPER = "urlParamMapper";
    private static final String URL = "url";
    DynamicUrlHandler dynamicUrlHandler = mock(DynamicUrlHandler.class);
    Map<String, String> properties = new HashMap();
    Map<String, String> urlContext = mock(HashMap.class);

    @Before
    public void setUp(){
        when(urlContext.get(PARAM_MAPPER)).thenReturn(PARAM_MAPPER);
        when(urlContext.get(URL)).thenReturn(URL);
    }

    @Ignore
    @Test(expected=NullPointerException.class)
    public void testBuildUrl() throws Exception {
        properties.put("prueba", "prueba");
        properties.put("prueba2", "prueba2");
        properties.put("prueba3", "prueba3");

//        dynamicUrlHandler = new DynamicUrlHandler();
//        dynamicUrlHandler.buildUrl(properties);
    }

    @Ignore
    @Test(expected=NullPointerException.class)
    public void testBuildUrl2() throws Exception {
        properties.put("prueba", "prueba");
        properties.put("prueba2", "prueba2");
        properties.put("prueba3", "prueba3");

//        dynamicUrlHandler = new DynamicUrlHandler();
//        dynamicUrlHandler.buildUrl(properties);
    }

//    public void testUpdateFilterParameters() throws Exception {
//
//    }
//
//    public void testConfigure() throws Exception {
//
//    }
}