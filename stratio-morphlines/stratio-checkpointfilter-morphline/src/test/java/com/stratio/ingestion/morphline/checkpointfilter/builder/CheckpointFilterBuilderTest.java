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
package com.stratio.ingestion.morphline.checkpointfilter.builder;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;

import com.google.common.io.Resources;
import com.typesafe.config.Config;

public class CheckpointFilterBuilderTest {

    private static final String MORPH_CONF_FILE = "checkpointfilter.conf";
    private static final String CHECKPOINT_FILTER = "checkpointFilter";
    private static final String COMMANDS = "commands";
    private Command command;
    private Config config;
    final MorphlineContext context = new MorphlineContext.Builder().build();
    Collector collectorParent = new Collector();
    Collector collectorChild = new Collector();

    @Before
    public void setUp() throws Exception {

        config = parse("checkpointfilter.conf").getConfigList(COMMANDS).get(0).getConfig(CHECKPOINT_FILTER);
        command = new CheckpointFilterBuilder().build(config, collectorParent,
                collectorChild, context);
    }

    @Test
    public void getNamesReturnsOk() throws Exception {
        assertThat(new CheckpointFilterBuilder().getNames()).containsOnly("checkpointFilter");
    }

    @Test
    public void recordMustPassTheFilterButNotCheckedAsCheckpoint() throws Exception {
        Record validRecord = new Record();
        validRecord.put("date", new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ssXXX").format(new Date(0)));
        int actualSize = validRecord.getFields().size();
        command.process(validRecord);
        assertThat(validRecord.getFields().size()).isEqualTo(actualSize);
    }

    @Test
    public void recordMustPassTheFilterAndIsCheckedAsCheckpoint() throws Exception {
        Record validRecord = new Record();
        validRecord.put("date", new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ssXXX").format(new Date(0)));
        int actualSize = validRecord.getFields().size();
        config = parse("checkpointfilter-2.conf").getConfigList(COMMANDS).get(0).getConfig(CHECKPOINT_FILTER);
        command = new CheckpointFilterBuilder().build(config, collectorParent,
                collectorChild, context);
        command.process(validRecord);
        assertThat(validRecord.getFields().size()).isEqualTo(actualSize+1);
    }

    protected Config parse(String file, Config... overrides) throws URISyntaxException, IOException {
        Config config = new org.kitesdk.morphline.base.Compiler().parse(
                new File(Resources.getResource(file).toURI()), overrides);
        config = checkNotNull(config.getConfigList("morphlines").get(0));
        return config;
    }
}