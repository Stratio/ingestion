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

import static com.stratio.ingestion.source.snmptraps.SNMPSourceConstants.*;

import java.io.IOException;
import java.util.Locale;

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
import org.snmp4j.security.Priv3DES;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.PrivAES192;
import org.snmp4j.security.PrivAES256;
import org.snmp4j.security.PrivDES;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.security.nonstandard.PrivAES192With3DESKeyExtension;
import org.snmp4j.security.nonstandard.PrivAES256With3DESKeyExtension;
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
    private int snmpTrapPort;
    private String username;
    private String password;
    private int version;
    private int trapVersion;
    private OID authMethod;
    private int securityMethod;
    private String privacyProtocol;
    private String privacyPassword;
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

        switch (Integer.valueOf(context.getString(CONF_SNMP_VERSION, DEFAULT_SNMP_VERSION)
                .replaceAll("(?i)V", "").replaceAll("(?i)C", ""))) {
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
                .getString(CONF_SNMP_TRAP_VERSION, DEFAULT_SNMP_TRAP_VERSION)
                .replaceAll("(?i)V", "").replaceAll("(?i)C", ""))) {
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

        final String hashAlgo = context.getString(CONF_ENCRYPTION, DEFAULT_ENCRYPTION);
        if ("SHA".equals(hashAlgo)) {
          authMethod = AuthSHA.ID;
        } else if ("MD5".equals(hashAlgo)) {
          authMethod = AuthMD5.ID;
        } else {
          authMethod = AuthMD5.ID;
        }

        final String auth = context.getString(CONF_AUTH, DEFAULT_AUTH);
        if ("AUTH_NOPRIV".equals(auth)) {
          securityMethod = SecurityLevel.AUTH_NOPRIV;
          username = context.getString(CONF_USERNAME);
          password = context.getString(CONF_PASSWD);
        } else if ("NOAUTH_NOPRIV".equals(auth)) {
          securityMethod = SecurityLevel.NOAUTH_NOPRIV;
        } else if ("AUTH_PRIV".equals(auth)) {
          securityMethod = SecurityLevel.AUTH_PRIV;
          username = context.getString(CONF_USERNAME);
          password = context.getString(CONF_PASSWD);
          privacyProtocol = context.getString(CONF_PRIV_PROTOCOL, DEFAULT_PRIV_PROTOCOL);
          privacyPassword = context.getString(CONF_PRIV_PASSPHRASE);
        } else {
          securityMethod = SecurityLevel.NOAUTH_NOPRIV;
        }

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

                // add auth and privacy
                UsmUser user = createUser();
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

                // add auth and Privacy
                UsmUser user = createUser();
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

    private UsmUser createUser() {
        OID privacyOID = null;
        OctetString privacyPasswd = null;
        if (securityMethod == SecurityLevel.AUTH_PRIV) {
            final String privacyProtocolUpper = privacyProtocol.toUpperCase(Locale.ENGLISH);
                if ("PRIVDES".equals(privacyProtocolUpper)) {
                  privacyOID = PrivDES.ID;
                } else if ("PRIV3DES".equals(privacyProtocolUpper)) {
                  privacyOID = Priv3DES.ID;
                } else if ("PRIVAES128".equals(privacyProtocolUpper)) {
                  privacyOID = PrivAES128.ID;
                } else if  ("PRIVAES192".equals(privacyProtocolUpper)) {
                  privacyOID = PrivAES192.ID;
                } else if ("PRIVAES256".equals(privacyProtocolUpper)) {
                  privacyOID = PrivAES256.ID;
                } else if  ("PRIVAES192WITH3DESKEYEXTENSION".equals(privacyProtocolUpper)) {
                  privacyOID = PrivAES192With3DESKeyExtension.ID;
                } else if ("PRIVAES256WITH3DESKEYEXTENSION".equals(privacyProtocolUpper)) {
                  privacyOID = PrivAES256With3DESKeyExtension.ID;
                } else {
                    log.debug("Privacy protocol " + privacyProtocolUpper + " unsupported or invalid.");
                }
            privacyPasswd = new OctetString(privacyPassword);
        }
        return new UsmUser(new OctetString(username), authMethod, new OctetString(password),
                privacyOID, privacyPasswd);
    }

}
