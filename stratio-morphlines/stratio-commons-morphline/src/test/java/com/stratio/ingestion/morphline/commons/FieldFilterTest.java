package com.stratio.ingestion.morphline.commons;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;

import static org.fest.assertions.Assertions.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@RunWith(JUnit4.class)
public class FieldFilterTest {

    @Before
    public void setUp() {
    }

    protected Config parse(String file, Config... overrides) throws IOException {
        File tmpFile = File.createTempFile("morphlines_", ".conf");
        IOUtils.copy(getClass().getResourceAsStream(file), new FileOutputStream(tmpFile));
        Config config = new Compiler().parse(tmpFile, overrides);
        config = config.getConfigList("morphlines").get(0);
        Preconditions.checkNotNull(config);
        return config;
    }

    private Record record(String...args) {
        final Record record = new Record();
        for (List<String> parts : Lists.partition(ImmutableList.copyOf(args), 2)) {
            record.put(parts.get(0), parts.get(1));
        }
        return record;
    }

    @Test
    public void onlyIncludeNoRegex() throws IOException {
        final MorphlineContext context =  new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new FieldFilterBuilder().build(
                parse("/onlyIncludeNoRegex.conf").getConfigList("commands").get(0).getConfig("fieldFilter"),
                collectorParent, collectorChild, context
        );

        Record record = record();
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record());
        collectorChild.reset();

        record = record(
                "field1", "value1",
                "field2", "value2",
                "field3", "value2"
        );
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record(
                "field1", "value1",
                "field2", "value2"
        ));
    }

    @Test
    public void onlyExcludeNoRegex() throws IOException {
        final MorphlineContext context =  new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new FieldFilterBuilder().build(
                parse("/onlyExcludeNoRegex.conf").getConfigList("commands").get(0).getConfig("fieldFilter"),
                collectorParent, collectorChild, context
        );

        Record record = record();
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record());
        collectorChild.reset();

        record = record(
                "field1", "value1",
                "field2", "value2",
                "field3", "value3"
        );
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record(
                "field3", "value3"
        ));
    }

    @Test
    public void includeAndExcludeNoRegex() throws IOException {
        final MorphlineContext context =  new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new FieldFilterBuilder().build(
                parse("/includeAndExcludeNoRegex.conf").getConfigList("commands").get(0).getConfig("fieldFilter"),
                collectorParent, collectorChild, context
        );

        Record record = record();
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record());
        collectorChild.reset();

        record = record(
                "field1", "value1",
                "field2", "value2",
                "field3", "value2"
        );
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record(
                "field1", "value1",
                "field2", "value2"
        ));
    }

    @Test
    public void onlyIncludeRegex() throws IOException {
        final MorphlineContext context =  new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new FieldFilterBuilder().build(
                parse("/onlyIncludeRegex.conf").getConfigList("commands").get(0).getConfig("fieldFilter"),
                collectorParent, collectorChild, context
        );

        Record record = record();
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record());
        collectorChild.reset();

        record = record(
                "field1", "value1",
                "field2", "value2",
                "field3", "value3"
        );
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record(
                "field1", "value1",
                "field2", "value2"
        ));

        collectorChild.reset();

        record = record(
                "field1", "value1",
                "field2", "value2",
                "field3", "value2",
                "fieldRegex1Blah", "value",
                "fieldRegex2Blah", "value"
        );
        command.process(record);
        assertThat(collectorChild.getFirstRecord()).isEqualTo(record(
                "field1", "value1",
                "field2", "value2",
                "fieldRegex1Blah", "value",
                "fieldRegex2Blah", "value"
        ));
    }

}
