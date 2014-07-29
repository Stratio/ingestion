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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

@RunWith(JUnit4.class)
public class CalculatorTest {

    private static final String ANY_FILE = "/readxml/test.xml";

    protected Config parse(String file, Config... overrides) throws IOException {
        File tmpFile = File.createTempFile("morphlines_", ".conf");
        IOUtils.copy(getClass().getResourceAsStream(file), new FileOutputStream(tmpFile));
        Config config = new Compiler().parse(tmpFile, overrides);
        config = config.getConfigList("morphlines").get(0);
        Preconditions.checkNotNull(config);
        return config;
    }

    @Test
    public void testBasicMultiply() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new CalculatorBuilder().build(parse("/calculator/basicMultiply.conf")
                .getConfigList("commands").get(0).getConfig("calculator"), collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("field1", 10);
        record.put(Fields.ATTACHMENT_BODY,
                new FileInputStream(new File(getClass().getResource(ANY_FILE).getPath())));

        command.process(record);
        Assert.assertEquals(collectorChild.getFirstRecord().getFirstValue("field2"),
                new BigDecimal(10 * 4));
    }

    @Test
    public void testSumFloat() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new CalculatorBuilder().build(parse("/calculator/sumFloat.conf")
                .getConfigList("commands").get(0).getConfig("calculator"), collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("field1", 10);
        record.put(Fields.ATTACHMENT_BODY,
                new FileInputStream(new File(getClass().getResource(ANY_FILE).getPath())));

        command.process(record);
        Assert.assertEquals(((BigDecimal) collectorChild.getFirstRecord().getFirstValue("field2"))
                .compareTo(new BigDecimal("14.4")), 0);
    }
    
    @Test
    public void testBasicPow() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new CalculatorBuilder().build(parse("/calculator/basicPow.conf")
                .getConfigList("commands").get(0).getConfig("calculator"), collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("field1", 10);
        record.put(Fields.ATTACHMENT_BODY,
                new FileInputStream(new File(getClass().getResource(ANY_FILE).getPath())));

        command.process(record);
        Assert.assertEquals(((BigDecimal) collectorChild.getFirstRecord().getFirstValue("output"))
                .compareTo(new BigDecimal("100")), 0);
    }
    
    @Test
    public void testSqrt() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new CalculatorBuilder().build(parse("/calculator/sqrt.conf")
                .getConfigList("commands").get(0).getConfig("calculator"), collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("field1", 16);
        record.put(Fields.ATTACHMENT_BODY,
                new FileInputStream(new File(getClass().getResource(ANY_FILE).getPath())));

        command.process(record);
        Assert.assertEquals(((BigDecimal) collectorChild.getFirstRecord().getFirstValue("output"))
                .compareTo(new BigDecimal("4")), 0);
    }
    
    @Test(expected = RuntimeException.class)
    public void testIncorrectOperator() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new CalculatorBuilder().build(parse("/calculator/incorrectOperator.conf")
                .getConfigList("commands").get(0).getConfig("calculator"), collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("field1", 16);
        record.put(Fields.ATTACHMENT_BODY,
                new FileInputStream(new File(getClass().getResource(ANY_FILE).getPath())));

        command.process(record);
    }
    
    @Test
    public void testBasicDivision() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new CalculatorBuilder().build(parse("/calculator/basicDivision.conf")
                .getConfigList("commands").get(0).getConfig("calculator"), collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put(Fields.ATTACHMENT_BODY,
                new FileInputStream(new File(getClass().getResource(ANY_FILE).getPath())));

        command.process(record);
        Assert.assertEquals(((BigDecimal) collectorChild.getFirstRecord().getFirstValue("output"))
                .compareTo(new BigDecimal("5")), 0);
    }
    
    @Test(expected = RuntimeException.class)
    public void testNonExistentField() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new CalculatorBuilder().build(parse("/calculator/NonExistentField.conf")
                .getConfigList("commands").get(0).getConfig("calculator"), collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("field1", 10);
        record.put(Fields.ATTACHMENT_BODY,
                new FileInputStream(new File(getClass().getResource(ANY_FILE).getPath())));

        command.process(record);
        Assert.assertEquals(((BigDecimal) collectorChild.getFirstRecord().getFirstValue("output"))
                .compareTo(new BigDecimal("5")), 0);
    }
}
