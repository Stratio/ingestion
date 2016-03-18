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
package com.stratio.ingestion.interceptor.metadata;

import com.stratio.metadata.entities.Json;
import com.stratio.metadata.javaapi.JavaMetaClient;
import com.stratio.metadata.model.ModelContainer;
import com.stratio.metadata.model.internals.schemaregistry.SchemaRegistry;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 * Add a new schema in Metadata Schema Registry
 *
 */
public class MetadataInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(MetadataInterceptor.class);

    private String zkConnection;
    private String schemaName;
    private String schema;

    public static final String ZK_CONNECTION = "zkConnection";
    public static final String SCHEMA_NAME = "schemaName";
    public static final String SCHEMA = "schema";

    protected MetadataInterceptor(Context context) {
        this.zkConnection = context.getString(ZK_CONNECTION, zkConnection);
        this.schema = context.getString(SCHEMA, "");
        this.schemaName = context.getString(SCHEMA_NAME, "");
    }

    // This method can be removed when MET-104 is solved
    public boolean existSchema(String schemaName, List<ModelContainer<SchemaRegistry>> schemas) {
        for (ModelContainer schema : schemas) {
            if (schema.id().equals(schemaName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void initialize() {
        try {
            JavaMetaClient metaClient = new JavaMetaClient(zkConnection);
            if (!existSchema(schemaName, metaClient.schemaregistry().list())) {
                metaClient.schemaregistry().create(schemaName, new SchemaRegistry(new Json(schema), "fullcheck"));
                logger.debug("New schema " + schemaName + " created");
            } else {
                metaClient.schemaregistry().update(schemaName, new SchemaRegistry(new Json(schema), "fullcheck"));
                logger.debug("Schema " + schemaName + " updated");
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            throw new ExceptionInInitializerError(ex.getMessage());
        }
    }

    @Override
    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }

    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    /** Builder implementations MUST have a public no-arg constructor */
    public static class Builder implements Interceptor.Builder {

        private Context context;

        public Builder() {
        }

        @Override
        public MetadataInterceptor build() {
            return new MetadataInterceptor(context);
        }

        @Override
        public void configure(Context context) {
            this.context = context;
        }

    }

}
