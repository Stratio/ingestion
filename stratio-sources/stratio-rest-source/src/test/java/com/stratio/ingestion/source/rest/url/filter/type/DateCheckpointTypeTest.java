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
package com.stratio.ingestion.source.rest.url.filter.type;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Created by miguelsegura on 30/09/15.
 */
public class DateCheckpointTypeTest extends TestCase {


    Map<String, String> context = mock(HashMap.class);
    private String CONF_DATE_PATTERN = "dateForma";
    Object fieldValue = new Date(0);
    Object objString = "2001-07-04T12:08:56.235-0700";
    Object currentCheckPoint = "2015-09-29T12:08:56.235-0700";
    Object currentBadCheckPoint = "2015-09-29T12:08:56.235-07";

    DateCheckpointType dateCheckpointType = new DateCheckpointType();

    @Before
    public void setUp() {
        when(context.get(CONF_DATE_PATTERN)).thenReturn(CONF_DATE_PATTERN);
        when(context.get("lastCheckpoint")).thenReturn("2001-09-04T12:08:56.235-0700");
    }

    @Test(expected = NullPointerException.class)
    public void testBuildFilter() throws Exception {
        String filter = dateCheckpointType.buildFilter(fieldValue, context);
    }

    @Test
    public void testBuildDefaultFilter() throws Exception {
        String filter = dateCheckpointType.buildDefaultFilter(context);
    }

    @Test(expected = ParseException.class)
    public void testParseFilter() throws Exception {
        Object filter = dateCheckpointType.parseFilter(objString, context);
    }

    @Test
    public void testIsValidCurrentCheckpoint() throws Exception {
        Boolean filter = dateCheckpointType.isValidCurrentCheckpoint(currentCheckPoint, context);
    }

    @Test(expected = ParseException.class)
    public void testIsNotValidCurrentCheckpoint() throws Exception {
        Boolean filter = dateCheckpointType.isValidCurrentCheckpoint(currentBadCheckPoint, context);
    }

}