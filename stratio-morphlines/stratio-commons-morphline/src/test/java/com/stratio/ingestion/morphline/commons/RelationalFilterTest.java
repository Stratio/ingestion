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
package com.stratio.ingestion.morphline.commons;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

public class RelationalFilterTest {
    
    private static final String MORPH_CONF_GREATER = "/relational/relationalGreater.conf";
    private static final String MORPH_CONF_GREATEREQUAL = "/relational/relationalGreaterEqual.conf";
    private static final String MORPH_CONF_LESS = "/relational/relationalLess.conf";
    private static final String MORPH_CONF_LESSEQUAL = "/relational/relationalLessEqual.conf";


    private Config configGreater, configGreaterEqual, configLess, configLessEqual;
    private Record record;
    List<Record> records;
    
    @Before
    public void setUp() throws IOException{
        configGreater = parse(MORPH_CONF_GREATER).getConfigList("commands").get(0).getConfig("relationalFilter");
        configGreaterEqual = parse(MORPH_CONF_GREATEREQUAL).getConfigList("commands").get(0).getConfig("relationalFilter");
        configLess = parse(MORPH_CONF_LESS).getConfigList("commands").get(0).getConfig("relationalFilter");
        configLessEqual = parse(MORPH_CONF_LESSEQUAL).getConfigList("commands").get(0).getConfig("relationalFilter");

        records = new ArrayList<Record>();
        int values[] = {1000,500,10,1,100};
        for(int i=0; i<5 ; i++){
            record = new Record();
            record.put("field1", values[i]);
            records.add(record);
        }
    }

    @Test
    public void testGreater(){
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new RelationalFilterBuilder().build(configGreater, collectorParent,
                collectorChild, context);

        processRecords(command, records);
        List<Record> records = collectorChild.getRecords();
        assertThat(records.size()).isEqualTo(2);
    }
    
    @Test
    public void testGreaterEqual(){
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new RelationalFilterBuilder().build(configGreaterEqual, collectorParent,
                collectorChild, context);

        processRecords(command, records);
        List<Record> records = collectorChild.getRecords();
        assertThat(records.size()).isEqualTo(3);
    }
    
    @Test
    public void testLess(){
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new RelationalFilterBuilder().build(configLess, collectorParent,
                collectorChild, context);

        processRecords(command, records);
        List<Record> records = collectorChild.getRecords();
        assertThat(records.size()).isEqualTo(4);
    }
    
    @Test
    public void testLessEqual(){
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new RelationalFilterBuilder().build(configLessEqual, collectorParent,
                collectorChild, context);

        processRecords(command, records);
        List<Record> records = collectorChild.getRecords();
        assertThat(records.size()).isEqualTo(5);
    }
    
    protected Config parse(String file, Config... overrides) throws IOException {
        File tmpFile = File.createTempFile("morphlines_", ".conf");
        IOUtils.copy(getClass().getResourceAsStream(file), new FileOutputStream(tmpFile));
        Config config = new Compiler().parse(tmpFile, overrides);
        config = config.getConfigList("morphlines").get(0);
        Preconditions.checkNotNull(config);
        return config;
    }
    
    private void processRecords(Command command, List<Record> records){
        for(Record record : records){
            command.process(record);
        }
    }
}
