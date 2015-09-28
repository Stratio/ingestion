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
 * Created by miguelsegura on 23/09/15.
 */

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.stratio.ingestion.model.AgentComponent;

import formatter.JsonFormatter;
import modeler.PropertiesModeler;


@RunWith(JUnit4.class)
public class PropertiesModelerTest {


//    @Test
//    public void testJson() throws IOException {
//
//        Prueba propModel = new Prueba();
//
//    }

    @Test
    public void testJsonLleno() throws IOException {

        PropertiesModeler propModel = new PropertiesModeler();

        AgentComponent agent = propModel.propertiesAgentModeler("src/test/resources/correctProp.properties");

        JsonFormatter jsonProp = new JsonFormatter(agent);

    }

    @Test(expected = NumberFormatException.class)
    public void testIntNull() throws IOException, NumberFormatException {

        PropertiesModeler propModel = new PropertiesModeler();

        AgentComponent agent = propModel.propertiesAgentModeler("src/test/resources/intNull.properties");

//        JsonFormatter jsonProp = new JsonFormatter(agent);

    }

    @Test(expected = IOException.class)
    public void testStringNull() throws IOException, NumberFormatException {

        PropertiesModeler propModel = new PropertiesModeler();

        AgentComponent agent = propModel.propertiesAgentModeler("src/test/resources/stringNull.properties");

//        JsonFormatter jsonProp = new JsonFormatter(agent);

    }

    @Test(expected = IOException.class)
    public void testIncorrectComp() throws IOException {

        PropertiesModeler propModel = new PropertiesModeler();

        AgentComponent agent = propModel.propertiesAgentModeler("src/test/resources/incorrectComp.properties");

        //        JsonFormatter jsonProp = new JsonFormatter(agent);

    }

    @Test(expected = IOException.class)
    public void testEmptyProp() throws IOException {

        PropertiesModeler propModel = new PropertiesModeler();

        AgentComponent agent = propModel.propertiesAgentModeler("src/test/resources/emptyProp.properties");

//        JsonFormatter jsonProp = new JsonFormatter(agent);

    }

    @Test(expected = IOException.class)
    public void testComponentsConnected() throws IOException {

        PropertiesModeler propModel = new PropertiesModeler();

        AgentComponent agent = propModel.propertiesAgentModeler("src/test/resources/notConnected.properties");
    }


}
