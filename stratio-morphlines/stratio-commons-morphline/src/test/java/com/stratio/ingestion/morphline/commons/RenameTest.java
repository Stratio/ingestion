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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

@RunWith(JUnit4.class)
public class RenameTest {
    
    private static final String MORPH_CONF_FILE = "/rename/rename.conf";
    
    private Config config;
    
    @Before
    public void setUp() throws IOException{
        config = parse(MORPH_CONF_FILE).getConfigList("commands").get(0).getConfig("rename");
    }
    
    @Test
    public void testRename(){
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new RenameBuilder().build(config, collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("field1", "content1");
        record.put("field2", "content2");
        
        command.process(record);
        List<Record> records = collectorChild.getRecords();
        
        assertEquals(records.size(),1);
        Record result = records.get(0);
        assertEquals(result.get("newfield1").get(0), "content1");
    }

    protected Config parse(String file, Config... overrides) throws IOException {
        File tmpFile = File.createTempFile("morphlines_", ".conf");
        IOUtils.copy(getClass().getResourceAsStream(file), new FileOutputStream(tmpFile));
        Config config = new Compiler().parse(tmpFile, overrides);
        config = config.getConfigList("morphlines").get(0);
        Preconditions.checkNotNull(config);
        return config;
    }
}
