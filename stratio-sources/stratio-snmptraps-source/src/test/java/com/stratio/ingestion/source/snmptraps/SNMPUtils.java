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

import java.io.IOException;

import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.ScopedPDU;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.UserTarget;
import org.snmp4j.mp.MPv3;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.PrivDES;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

public class SNMPUtils {
    public static void sendTrapV1(String port) throws IOException {

        TransportMapping<?> transport = new DefaultUdpTransportMapping();
        transport.listen();

        CommunityTarget comtarget = new CommunityTarget();
        comtarget.setCommunity(new OctetString(new OctetString("public")));
        comtarget.setVersion(SnmpConstants.version1);
        comtarget.setAddress(new UdpAddress("127.0.0.1/" + port));
        comtarget.setRetries(2);
        comtarget.setTimeout(5000);

        PDU trap = new PDUv1();
        trap.setType(PDU.V1TRAP);

        OID oid = new OID("1.2.3.4.5");
        trap.add(new VariableBinding(SnmpConstants.snmpTrapOID, oid));
        trap.add(new VariableBinding(SnmpConstants.sysUpTime, new TimeTicks(5000)));
        trap.add(new VariableBinding(SnmpConstants.sysDescr, new OctetString("System Description")));

        // Add Payload
        Variable var = new OctetString("some string");
        trap.add(new VariableBinding(oid, var));

        // Send
        Snmp snmp = new Snmp(transport);
        snmp.send(trap, comtarget);
        transport.close();
        snmp.close();

    }

    public static void sendTrapV2(String port) throws IOException {
        PDU trap = new PDU();
        trap.setType(PDU.TRAP);

        OID oid = new OID("1.2.3.4.5");
        trap.add(new VariableBinding(SnmpConstants.snmpTrapOID, oid));
        trap.add(new VariableBinding(SnmpConstants.sysUpTime, new TimeTicks(5000)));
        trap.add(new VariableBinding(SnmpConstants.sysDescr, new OctetString("System Description")));

        // Add Payload
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

        snmp.close();
    }

    public static void sendTrapV3(String port) {
        try {
            Address targetAddress = GenericAddress.parse("udp:127.0.0.1/" + port);
            TransportMapping<?> transport = new DefaultUdpTransportMapping();
            Snmp snmp = new Snmp(transport);
            USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(
                    MPv3.createLocalEngineID()), 0);
            SecurityModels.getInstance().addSecurityModel(usm);
            transport.listen();

            snmp.getUSM().addUser(new OctetString("MD5DES"),
                    new UsmUser(new OctetString("MD5DES"), null, null, null, null));

            // Create Target
            UserTarget target = new UserTarget();
            target.setAddress(targetAddress);
            target.setRetries(1);
            target.setTimeout(11500);
            target.setVersion(SnmpConstants.version3);
            target.setSecurityLevel(SecurityLevel.NOAUTH_NOPRIV);
            target.setSecurityName(new OctetString("MD5DES"));

            // Create PDU for V3
            ScopedPDU pdu = new ScopedPDU();
            pdu.setType(ScopedPDU.NOTIFICATION);
            pdu.add(new VariableBinding(SnmpConstants.sysUpTime));
            pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, SnmpConstants.linkDown));
            pdu.add(new VariableBinding(new OID("1.2.3.4.5"), new OctetString("Major")));

            // Send the PDU
            snmp.send(pdu, target);

            transport.close();
            snmp.close();
        } catch (Exception e) {
            System.err.println("Error in Sending Trap to (IP:Port)=> " + "127.0.0.1" + ":" + port);
            System.err.println("Exception Message = " + e.getMessage());
        }
    }

    public static void sendTrapV3Auth(String port) throws IOException {
        try {
            Address targetAddress = GenericAddress.parse("udp:127.0.0.1/" + port);
            TransportMapping<?> transport = new DefaultUdpTransportMapping();
            Snmp snmp = new Snmp(transport);
            USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(
                    MPv3.createLocalEngineID()), 0);
            SecurityModels.getInstance().addSecurityModel(usm);
            transport.listen();

            snmp.getUSM().addUser(
                    new OctetString("user"),
                    new UsmUser(new OctetString("user"), AuthMD5.ID, new OctetString("12345678"),
                            null, null));

            // Create Target
            UserTarget target = new UserTarget();
            target.setAddress(targetAddress);
            target.setRetries(1);
            target.setTimeout(11500);
            target.setVersion(SnmpConstants.version3);
            target.setSecurityLevel(SecurityLevel.AUTH_NOPRIV);
            target.setSecurityName(new OctetString("user"));

            // Create PDU for V3
            ScopedPDU pdu = new ScopedPDU();
            pdu.setType(ScopedPDU.NOTIFICATION);
            pdu.add(new VariableBinding(SnmpConstants.sysUpTime));
            pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, SnmpConstants.linkDown));
            pdu.add(new VariableBinding(new OID("1.2.3.4.5"), new OctetString("Major")));

            // Send the PDU
            snmp.send(pdu, target);

            transport.close();
            snmp.close();
        } catch (Exception e) {
            System.err.println("Error in Sending Trap to (IP:Port)=> " + "127.0.0.1" + ":" + port);
            System.err.println("Exception Message = " + e.getMessage());
        }
    }

    public static void sendTrapV3AuthPriv(String port) throws IOException {
        try {
            Address targetAddress = GenericAddress.parse("udp:127.0.0.1/" + port);
            TransportMapping<?> transport = new DefaultUdpTransportMapping();
            Snmp snmp = new Snmp(transport);
            USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(
                    MPv3.createLocalEngineID()), 0);
            SecurityModels.getInstance().addSecurityModel(usm);
            transport.listen();

            snmp.getUSM().addUser(
                    new OctetString("user"),
                    new UsmUser(new OctetString("user"), AuthMD5.ID, new OctetString("12345678"),
                            PrivDES.ID, new OctetString("passphrase")));

            // Create Target
            UserTarget target = new UserTarget();
            target.setAddress(targetAddress);
            target.setRetries(1);
            target.setTimeout(11500);
            target.setVersion(SnmpConstants.version3);
            target.setSecurityLevel(SecurityLevel.AUTH_NOPRIV);
            target.setSecurityName(new OctetString("user"));

            // Create PDU for V3
            ScopedPDU pdu = new ScopedPDU();
            pdu.setType(ScopedPDU.NOTIFICATION);
            pdu.add(new VariableBinding(SnmpConstants.sysUpTime));
            pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, SnmpConstants.linkDown));
            pdu.add(new VariableBinding(new OID("1.2.3.4.5"), new OctetString("Major")));

            // Send the PDU
            snmp.send(pdu, target);

            transport.close();
            snmp.close();
        } catch (Exception e) {
            System.err.println("Error in Sending Trap to (IP:Port)=> " + "127.0.0.1" + ":" + port);
            System.err.println("Exception Message = " + e.getMessage());
        }
    }
}
