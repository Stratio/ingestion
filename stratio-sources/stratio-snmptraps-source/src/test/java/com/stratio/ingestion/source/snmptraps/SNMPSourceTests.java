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

import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_TRAP_PORT;

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
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

@RunWith(JUnit4.class)
public class SNMPSourceTests {
    
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
    public void testBasic() throws InterruptedException, IOException {
        Context context = new Context();
        context.put(CONF_TRAP_PORT, STMP_TRAP_PORT);

        Configurables.configure(source, context);
        source.start();

        while (source.getSourceCounter().getEventAcceptedCount() < 1) {
            sendTrapV2(STMP_TRAP_PORT);
            Thread.sleep(10);
        }
        
        Transaction txn = channel.getTransaction();
        txn.begin();
        Event e = channel.take();
        Assert.assertNotNull("Event must not be null", e);
        Assert.assertNotNull("Event headers must not be null", e.getBody());
        txn.commit();
        txn.close();
    }
    
    private void sendTrapV2(String port) throws IOException {
        PDU trap = new PDU();
        trap.setType(PDU.TRAP);

        OID oid = new OID("1.2.3.4.5");
        trap.add(new VariableBinding(SnmpConstants.snmpTrapOID, oid));
        trap.add(new VariableBinding(SnmpConstants.sysUpTime, new TimeTicks(5000))); // put your uptime here
        trap.add(new VariableBinding(SnmpConstants.sysDescr, new OctetString("System Description"))); 

        //Add Payload
        Variable var = new OctetString("some string");          
        trap.add(new VariableBinding(oid, var));          

        // Specify receiver
        Address targetaddress = new UdpAddress("127.0.0.1/" + port);
        CommunityTarget target = new CommunityTarget();
        target.setCommunity(new OctetString("public"));
        target.setVersion(SnmpConstants.version2c);
        target.setAddress(targetaddress);

        // Send
        Snmp snmp = new Snmp(new DefaultUdpTransportMapping());
        snmp.send(trap, target, null, null);  
    }
}
