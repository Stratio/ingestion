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

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@RunWith(JUnit4.class)
public class MetadataInterceptorIT extends Assert {

    private static final Logger logger = LoggerFactory.getLogger(MetadataInterceptorIT.class);

    private static ZKUtils zkUtils;
    private static String METADATA_PATH = "/stratio/metadata/schemaregistry";

    @Before
    public void setUp() throws Exception {
        Config conf = ConfigFactory.load();

        String zk_hosts = StringUtils.join(conf.getStringList("zookeeper.hosts"), ",");
        zkUtils = new ZKUtils(zk_hosts);

        logger.debug("Using Zookeeper hosts: " + zk_hosts);
    }

    private MetadataInterceptor build(Context context) {
        MetadataInterceptor.Builder builder = new MetadataInterceptor.Builder();
        builder.configure(context);
        return builder.build();
    }

    @Test(expected = ExceptionInInitializerError.class)
    public void test() throws Exception {
        logger.debug("This test should be return an Error");
        Context context = new Context();
        context.put("zkConnection", "127.0.0.1:2181");
        context.put("schemaName", "schemaTest");
        context.put("schema", "not valid Json");
        build(context).initialize();
    }

    @Test
    public void test2() throws Exception {
        logger.debug("This test should create a new schema");
        Context context = new Context();
        context.put("zkConnection", "127.0.0.1:2181");
        context.put("schemaName", "schemaTest");
        context.put("schema", "{ \"name\" : \"value\" }");
        build(context).initialize();
        assertTrue(zkUtils.existZNode(METADATA_PATH + "/" + "schemaTest"));
    }

    @AfterClass
    public static void shutDown() throws Exception {
        zkUtils.removeZNode(METADATA_PATH + "/" + "schemaTest");
        zkUtils.close();
    }

}
