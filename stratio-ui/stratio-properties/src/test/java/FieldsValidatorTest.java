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
/**
 * Created by miguelsegura on 15/09/15.
 */

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import jsonFormatter.FieldsValidator;

@RunWith(JUnit4.class)
public class FieldsValidatorTest
{
    //    public JsonFormatterTest() {}

    //    private ResettableInputStream getTestInputStream()
    //            throws Exception
    //    {
    //        return getTestInputStream("lacteos.json");
    //    }
    //
    //        private ResettableInputStream getTestInputStream(String path)
    //                throws Exception
    //        {
    //            File f = new File(getClass().getClassLoader().getResource(path).toURI());
    //            return new ResettableFileInputStream(f, new TransientPositionTracker("dummy"));
    //        }

    @Test
    public void testJsonLleno() throws IOException {
        FieldsValidator jsForm = new FieldsValidator("/home/miguelsegura/workspace/flume-ingestion/stratio-ui/stratio-properties"
                + "/src/test/resources"
                        + "/ejemploProp.json");
        //        JsonFormatter jsForm = new JsonFormatter(context, "../../../test/resources/ejemploSource.json");

        //        validateReadAndMark(des);
    }

//    @Test(expected = IOException.class)
//    public void testJsonVacio() throws IOException {
//        FieldsValidator jsForm = new FieldsValidator("/home/miguelsegura/workspace/flume-ingestion/stratio-ui/stratio-properties/src/test/resources"
//                        + "/jsonVacio.json");
//        //        JsonFormatter jsForm = new JsonFormatter(context, "../../../test/resources/ejemploSource.json");
//
//        //        validateReadAndMark(des);
//    }

//    @Test
//    public void testJsonFields() throws IOException {
//        FieldsValidator jsForm = new FieldsValidator(
//                "/home/miguelsegura/workspace/flume-ingestion/stratio-ui/stratio-properties"
//                + "/src/test/resources"
//                        + "/jsonVacio.json");
//        //        JsonFormatter jsForm = new JsonFormatter(context, "../../../test/resources/ejemploSource.json");
//
//        //        validateReadAndMark(des);
//    }


}