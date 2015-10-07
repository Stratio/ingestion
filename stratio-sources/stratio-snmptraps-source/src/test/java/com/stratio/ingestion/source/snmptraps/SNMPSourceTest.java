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
package com.stratio.ingestion.source.snmptraps;

import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_AUTH;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_PASSWD;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_PRIV_PASSPHRASE;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_PRIV_PROTOCOL;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_SNMP_TRAP_VERSION;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_TRAP_PORT;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_USERNAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SNMPSourceTest {

    private static final String STMP_TRAP_PORT = "1620";

    static SNMPSource source;
    static MemoryChannel channel;

    @Before
    public void setUp() {
        source = new SNMPSource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
    }

    @Test
//    @Ignore
    public void testV1NoAuth() throws InterruptedException, IOException {
        Context context = new Context();
        context.put(CONF_TRAP_PORT, STMP_TRAP_PORT);
        context.put(CONF_SNMP_TRAP_VERSION, "V1");

        Configurables.configure(source, context);
        source.start();

        while (source.getSourceCounter().getEventAcceptedCount() < 1) {
            SNMPUtils.sendTrapV1(STMP_TRAP_PORT);
            Thread.sleep(10);
        }

        checkEventsChannel();
    }

    @Test
//    @Ignore
    public void testV2NoAuth() throws InterruptedException, IOException {
        Context context = new Context();
        context.put(CONF_TRAP_PORT, STMP_TRAP_PORT);
        context.put(CONF_SNMP_TRAP_VERSION, "v2C");

        Configurables.configure(source, context);
        source.start();

        while (source.getSourceCounter().getEventAcceptedCount() < 1) {
            SNMPUtils.sendTrapV2(STMP_TRAP_PORT);
            Thread.sleep(10);
        }

        checkEventsChannel();
    }

    @Test
//    @Ignore
    public void testV3NoAuth() throws InterruptedException, IOException {
        Context context = new Context();
        context.put(CONF_TRAP_PORT, STMP_TRAP_PORT);
        context.put(CONF_SNMP_TRAP_VERSION, "v2C");

        Configurables.configure(source, context);
        source.start();

        while (source.getSourceCounter().getEventAcceptedCount() < 1) {
            SNMPUtils.sendTrapV3(STMP_TRAP_PORT);
            Thread.sleep(10);
        }

        checkEventsChannel();
    }

    @Test
//    @Ignore
    public void testV3Auth() throws IOException, InterruptedException {
        Context context = new Context();
        context.put(CONF_TRAP_PORT, STMP_TRAP_PORT);
        context.put(CONF_SNMP_TRAP_VERSION, "V3");
        context.put(CONF_AUTH, "AUTH_NOPRIV");
        context.put(CONF_USERNAME, "user");
        context.put(CONF_PASSWD, "12345678");

        Configurables.configure(source, context);
        source.start();

        while (source.getSourceCounter().getEventAcceptedCount() < 1) {
            SNMPUtils.sendTrapV3Auth(STMP_TRAP_PORT);
            Thread.sleep(10);
        }

        checkEventsChannel();
    }

    @Test
//    @Ignore
    public void testV3AuthPriv() throws IOException, InterruptedException {
        Context context = new Context();
        context.put(CONF_TRAP_PORT, STMP_TRAP_PORT);
        context.put(CONF_SNMP_TRAP_VERSION, "V3");
        context.put(CONF_AUTH, "AUTH_PRIV");
        context.put(CONF_USERNAME, "user");
        context.put(CONF_PASSWD, "12345678");
        context.put(CONF_PRIV_PROTOCOL, "PrivDES");
        context.put(CONF_PRIV_PASSPHRASE, "passphrase");

        Configurables.configure(source, context);
        source.start();

        while (source.getSourceCounter().getEventAcceptedCount() < 1) {
            SNMPUtils.sendTrapV3Auth(STMP_TRAP_PORT);
            Thread.sleep(10);
        }

        checkEventsChannel();
    }

    public void checkEventsChannel() {
        Transaction txn = channel.getTransaction();
        txn.begin();
        Event e = channel.take();
        Assert.assertNotNull("Event must not be null", e);
        Assert.assertNotNull("Event headers must not be null", e.getBody());
        txn.commit();
        txn.close();
        source.stop();
    }

}
