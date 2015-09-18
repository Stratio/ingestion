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

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import jsonFormatter.JsonFormatter;

@RunWith(JUnit4.class)
public class JsonFormatterTest
{

    @Test
    public void testJsonLleno() throws IOException {

        JsonFormatter jsForm = new JsonFormatter("/home/miguelsegura/workspace/flume-ingestion/stratio-ui/stratio-properties/src/test/resources"
                        + "/ejemploProp.json");

    }

    @Test(expected = IOException.class)
    public void testJsonVacio() throws IOException {

        JsonFormatter jsForm = new JsonFormatter("/home/miguelsegura/workspace/flume-ingestion/stratio-ui/stratio-properties/src/test/resources"
                        + "/jsonVacio.json");
    }

//        System.out.println((String)evt.getHeaders().get("myHeader"));
//        Assert.assertTrue(((String)evt.getHeaders().get("myHeader")).contains("Nigel Rees"));
//        des.mark();
//
//        evt = des.readEvent();
//        Assert.assertTrue(((String)evt.getHeaders().get("myHeader")).contains("Evelyn Waugh"));
//        des.mark();
//
//        List<Event> readEvents = des.readEvents(2);
//        Assert.assertEquals(2L, readEvents.size());
//
//        evt = des.readEvent();
//        Assert.assertNull("Event should be null because there are no more books left to read", evt);
//
//        des.mark();
//        des.mark();
//        des.close();
//    }
}
