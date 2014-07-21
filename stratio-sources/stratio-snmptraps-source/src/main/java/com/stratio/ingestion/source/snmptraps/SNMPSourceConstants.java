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

public class SNMPSourceConstants {

    public static final String CONF_ADDRESS = "address";
    public static final String CONF_TRAP_PORT = "snmpTrapPort";
    public static final String CONF_SNMP_VERSION = "snmpVersion";
    public static final String CONF_SNMP_TRAP_VERSION = "snmpTrapVersion";
    public static final String CONF_AUTH = "authenticationType";
    public static final String CONF_USERNAME = "username";
    public static final String CONF_PASSWD = "password";
    public static final String CONF_ENCRYPTION = "encryptionType";
    public static final String CONF_PRIV_PROTOCOL = "privacyProtocol";
    public static final String CONF_PRIV_PASSPHRASE = "privacyPassprhase";

    public static final String DEFAULT_ADDRESS = "localhost";
    public static final Integer DEFAULT_SNMP_PORT = 161;
    public static final Integer DEFAULT_TRAP_PORT = 162;
    public static final String DEFAULT_SNMP_VERSION = "V1";
    public static final String DEFAULT_SNMP_TRAP_VERSION = "V1";
    public static final String DEFAULT_ENCRYPTION = "MD5";
    public static final String DEFAULT_AUTH = "NOAUTH_NOPRIV";
    public static final String DEFAULT_PRIV_PROTOCOL = "PrivDES";
}
