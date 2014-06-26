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
package com.stratio.ingestion.source.generator;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Random;

public class RandomGeneratorSource extends AbstractSource implements
        Configurable, PollableSource {

    private static final Logger logger = LoggerFactory
            .getLogger(RandomGeneratorSource.class);
    private static final String STREAM_DEFINITION_FILE = "generatorDefinitionFile";

    private long sequence;
    private CounterGroup counterGroup;
    private List<GeneratorField> generatorFields;

    public RandomGeneratorSource() {
        counterGroup = new CounterGroup();
    }

    @Override
    public void configure(Context context) {
        String columnDefinitionFile = context.getString(STREAM_DEFINITION_FILE);
        RandomGeneratorDefinitionParser parser = new RandomGeneratorDefinitionParser(readJsonFromFile(new File(columnDefinitionFile)));
        GeneratorDefinition theStreamDefinition = parser.parse();
        this.generatorFields = theStreamDefinition.getFields();
    }

    @Override
    public Status process() throws EventDeliveryException {

        try {
            String randomStringEvent = createRandomStringEvent();
            getChannelProcessor().processEvent(
                    EventBuilder.withBody(String.valueOf(randomStringEvent).getBytes()));
            counterGroup.incrementAndGet("events.successful");
        } catch (ChannelException ex) {
            counterGroup.incrementAndGet("events.failed");
        }

        return Status.READY;
    }

    private String createRandomStringEvent() {
        StringBuilder randomString = new StringBuilder();
        for (GeneratorField field: generatorFields) {
            randomString.append(RandomFieldsGenerator.generateRandomField(field));
        }
        return randomString.toString();
    }

    @Override
    public void start() {
        logger.info("Stratio random generator source starting");

        super.start();

        logger.debug("Stratio random generator source started");
    }

    @Override
    public void stop() {
        logger.info("Stratio random generator source stopping");

        super.stop();

        logger.info("Stratio random generator source stopped. Metrics:{}", counterGroup);
    }

    private static final String readJsonFromFile(File file) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(file);
            return IOUtils.toString(inputStream, "UTF-8");
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

}
