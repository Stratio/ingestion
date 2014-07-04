package com.stratio.ingestion.morphline.commons;

import static org.fest.assertions.Assertions.assertThat;

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
public class TimeFilterTest {

    private static final String MORPH_CONF_FILE = "/timefilter/timeFilter.conf";

    private Config config;

    @Before
    public void setUp() throws IOException {
        config = parse(MORPH_CONF_FILE).getConfigList("commands").get(0).getConfig("timeFilter");
    }

    @Test
    public void testFilterOk() {

        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new TimeFilterBuilder().build(config, collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("createdAt", "21/01/2014");
        
        command.process(record);
        
        List<Record> records = collectorChild.getRecords();
        assertThat(records.size()).isGreaterThan(0);
    }

    @Test
    public void testDiscardRecord() {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new TimeFilterBuilder().build(config, collectorParent,
                collectorChild, context);

        Record record = new Record();
        record.put("createdAt", "21/01/2015");
        
        command.process(record);
        
        List<Record> records = collectorChild.getRecords();
        assertThat(records.size()).isEqualTo(0);
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
