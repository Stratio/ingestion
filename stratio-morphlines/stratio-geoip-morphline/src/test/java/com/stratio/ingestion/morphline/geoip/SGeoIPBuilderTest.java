
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
package com.stratio.ingestion.morphline.geoip;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by epeinado on 27/05/15.
 */
@RunWith(JUnit4.class)
public class SGeoIPBuilderTest {

    protected Config parse(String file, Config... overrides) throws IOException {
        File tmpFile = File.createTempFile("morphlines_", ".conf");
        IOUtils.copy(getClass().getResourceAsStream(file), new FileOutputStream(tmpFile));
        Config config = new org.kitesdk.morphline.base.Compiler().parse(tmpFile, overrides);
        config = config.getConfigList("morphlines").get(0);
        Preconditions.checkNotNull(config);
        return config;
    }

    @Test
    public void test1() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new SGeoIPBuilder().build(parse("/conf/basicConf.conf")
                .getConfigList("commands").get(0).getConfig("sgeoIP"), collectorParent,
        collectorChild, context);

        Record record = new Record();
        record.put("log_host", "66.102.11.104");

        command.process(record);
        Assert.assertEquals(collectorChild.getFirstRecord().get("log_iso_code").get(0),
                "US");
    }

}
