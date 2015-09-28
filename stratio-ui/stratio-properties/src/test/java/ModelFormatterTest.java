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
 * Created by miguelsegura on 22/09/15.
 */

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.stratio.ingestion.model.AgentComponent;

import formatter.ModelFormatter;
import modeler.JsonModeler;


@RunWith(JUnit4.class)
public class ModelFormatterTest {

    @Test
    public void testJsonCorrect() throws IOException {

        JsonModeler jsModel = new JsonModeler("src/test/resources"
                + "/jsonPropertiesCorrect.json");

        AgentComponent agent = jsModel.getAgent();

        ModelFormatter modelProperties = new ModelFormatter(agent);

    }

    @Test(expected = IOException.class)
    public void testJsonEmpty() throws IOException {

        JsonModeler jsModel = new JsonModeler("src/test/resources"
                + "/jsonEmpty.json");

        AgentComponent agent = jsModel.getAgent();

        ModelFormatter modelProperties = new ModelFormatter(agent);

    }

    @Test(expected = IOException.class)
    public void testJsonIncorrectConnection() throws IOException {

        JsonModeler jsModel = new JsonModeler("src/test/resources"
                + "/jsonPropertiesConnectWrong.json");

        AgentComponent agent = jsModel.getAgent();

        ModelFormatter modelProperties = new ModelFormatter(agent);

    }

    @Test(expected = IOException.class)
    public void testJsonIncorrectRequired() throws IOException {

        JsonModeler jsModel = new JsonModeler("src/test/resources"
                + "/jsonPropertiesReqWrong.json");

        AgentComponent agent = jsModel.getAgent();

        ModelFormatter modelProperties = new ModelFormatter(agent);

    }
}



