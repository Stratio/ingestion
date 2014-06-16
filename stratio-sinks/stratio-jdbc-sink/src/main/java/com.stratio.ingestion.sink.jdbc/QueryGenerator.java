package com.stratio.ingestion.sink.jdbc;

import org.apache.flume.Event;
import org.jooq.DSLContext;

import java.util.List;

public interface QueryGenerator {

    boolean executeQuery(DSLContext dslContext, List<Event> event);

}
