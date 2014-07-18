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

import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_ADDRESS;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_AUTH;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_COMMUNITY;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_ENCRYPTION;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_PASSWD;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_SNMP_PORT;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_SNMP_TRAP_VERSION;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_SNMP_VERSION;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_TRAP_PORT;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.CONF_USERNAME;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.DEFAULT_ADDRESS;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.DEFAULT_AUTH;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.DEFAULT_ENCRYPTION;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.DEFAULT_SNMP_PORT;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.DEFAULT_SNMP_TRAP_VERSION;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.DEFAULT_SNMP_VERSION;
import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.DEFAULT_TRAP_PORT;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.MessageDispatcher;
import org.snmp4j.MessageDispatcherImpl;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.mp.MPv3;
import org.snmp4j.mp.MessageProcessingModel;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.AbstractTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

public class SNMPSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger log = LoggerFactory.getLogger(SNMPSource.class);

    private String address;
    private int snmpPort;
    private int snmpTrapPort;
    private String username;
    private String password;
    private int version;
    private int trapVersion;
    private OID authMethod;
    private int securityMethod;
    private String community;
    private long timeout = 10000; // default 10 seconds
    private int retries = 2; // default 2 retries
    private SourceCounter sourceCounter;

    /** SNMP4J stuff **/
    private AbstractTransportMapping<UdpAddress> transport;
    private Snmp snmp;

    /** SNMP4J trap stuff **/
    private AbstractTransportMapping<UdpAddress> trap_transport;
    private Snmp trap_snmp;

    @Override
    public void configure(Context context) {
        address = context.getString(CONF_ADDRESS, DEFAULT_ADDRESS);
        snmpTrapPort = context.getInteger(CONF_TRAP_PORT, DEFAULT_TRAP_PORT);
        snmpPort = context.getInteger(CONF_SNMP_PORT, DEFAULT_SNMP_PORT);

        switch (Integer.valueOf(context.getString(CONF_SNMP_VERSION, DEFAULT_SNMP_VERSION)
                .replace("V", "").replace("C", ""))) {
            case 1:
                version = SnmpConstants.version1;
                break;
            case 2:
                version = SnmpConstants.version2c;
                break;
            case 3:
                version = SnmpConstants.version3;
                break;
            default:
                version = SnmpConstants.version1;
                break;
        }

        switch (Integer.valueOf(context
                .getString(CONF_SNMP_TRAP_VERSION, DEFAULT_SNMP_TRAP_VERSION).replace("V", "")
                .replace("C", ""))) {
            case 1:
                trapVersion = SnmpConstants.version1;
                break;
            case 2:
                trapVersion = SnmpConstants.version2c;
                break;
            case 3:
                trapVersion = SnmpConstants.version3;
                break;
            default:
                trapVersion = SnmpConstants.version1;
                break;
        }

        switch (context.getString(CONF_ENCRYPTION, DEFAULT_ENCRYPTION)) {
            case "SHA":
                authMethod = AuthSHA.ID;
                break;
            case "MD5":
                authMethod = AuthMD5.ID;
                break;
            default:
                authMethod = AuthMD5.ID;
                break;
        }

        switch (context.getString(CONF_AUTH, DEFAULT_AUTH)) {
            case "AUTH_NOPRIV":
                securityMethod = SecurityLevel.AUTH_NOPRIV;
                break;
            case "NOAUTH_NOPRIV":
                securityMethod = SecurityLevel.NOAUTH_NOPRIV;
                break;
            case "AUTH_PRIV":
                securityMethod = SecurityLevel.AUTH_PRIV;
                break;
            default:
                securityMethod = SecurityLevel.NOAUTH_NOPRIV;
                break;
        }

        username = context.getString(CONF_USERNAME);
        password = context.getString(CONF_PASSWD);
        community = context.getString(CONF_COMMUNITY);

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @Override
    public void start() {
        try {

            // --------------------------------------------------
            // POLLING
            // --------------------------------------------------
            transport = new DefaultUdpTransportMapping();
            if (version == SnmpConstants.version3) {
                snmp = new Snmp(transport);

                // add security model
                @SuppressWarnings("static-access")
                byte[] localEngineID = ((MPv3) snmp
                        .getMessageProcessingModel(MessageProcessingModel.MPv3))
                        .createLocalEngineID();
                USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(localEngineID),
                        0);
                SecurityModels.getInstance().addSecurityModel(usm);
                snmp.setLocalEngine(localEngineID, 0, 0);

                // add auth
                UsmUser user = new UsmUser(new OctetString(username), authMethod, new OctetString(
                        password), null, null);
                snmp.getUSM().addUser(new OctetString(username), user);

            } else {
                MessageDispatcher mDispatcher = new MessageDispatcherImpl();
                mDispatcher.addMessageProcessingModel(new MPv1());
                mDispatcher.addMessageProcessingModel(new MPv2c());
                mDispatcher.addMessageProcessingModel(new MPv3());

                snmp = new Snmp(mDispatcher, transport);
            }

            transport.listen();

            // --------------------------------------------------
            // TRAPS
            // --------------------------------------------------

            UdpAddress trapUdpAddress = new UdpAddress("0.0.0.0/" + snmpTrapPort);
            trap_transport = new DefaultUdpTransportMapping(trapUdpAddress);

            if (trapVersion == SnmpConstants.version3) {

                trap_snmp = new Snmp(trap_transport);

                // add security model
                @SuppressWarnings("static-access")
                byte[] localEngineID = ((MPv3) trap_snmp
                        .getMessageProcessingModel(MessageProcessingModel.MPv3))
                        .createLocalEngineID();
                USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(localEngineID),
                        0);
                SecurityModels.getInstance().addSecurityModel(usm);
                trap_snmp.setLocalEngine(localEngineID, 0, 0);

                // add auth
                UsmUser user = new UsmUser(new OctetString(username), authMethod, new OctetString(
                        password), null, null);
                trap_snmp.getUSM().addUser(new OctetString(username), user);

            } else {
                MessageDispatcher mDispatcher = new MessageDispatcherImpl();
                mDispatcher.addMessageProcessingModel(new MPv1());
                mDispatcher.addMessageProcessingModel(new MPv2c());
                mDispatcher.addMessageProcessingModel(new MPv3());

                trap_snmp = new Snmp(mDispatcher, trap_transport);
            }

            CommandResponder trapsCatch = new CommandResponder() {
                public synchronized void processPdu(CommandResponderEvent e) {
                    PDU command = e.getPDU();
                    if (command != null) {
                        ChannelProcessor channelProcessor = getChannelProcessor();
                        sourceCounter.addToEventReceivedCount(1);
                        sourceCounter.incrementAppendBatchReceivedCount();
                        channelProcessor.processEvent(EventBuilder.withBody(command.toString(),
                                Charsets.UTF_8));
                        sourceCounter.addToEventAcceptedCount(1);
                        sourceCounter.incrementAppendBatchAcceptedCount();
                    }
                }
            };

            trap_snmp.addCommandResponder(trapsCatch);
            trap_transport.listen();

            // --------------------------------------------------

            log.debug("[SNMP] SNMP Trap binding is listening on " + address);

        } catch (IOException e) {
            log.debug("couldn't listen to " + address + "----" + e);
            e.printStackTrace();
        }
        
        sourceCounter.start();

    }

    @Override
    public void stop() {
        log.debug("Closing SNMP connection with: " + this.address);
        try {
            snmp.close();
            trap_snmp.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
      return sourceCounter;
    }

}
