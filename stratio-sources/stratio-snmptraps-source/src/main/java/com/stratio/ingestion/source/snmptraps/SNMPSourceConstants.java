package com.stratio.ingestion.source.snmptraps;


public class SNMPSourceConstants {

    public static final String CONF_ADDRESS = "address";
    public static final String CONF_SNMP_PORT = "snmpPort";
    public static final String CONF_TRAP_PORT = "snmpTrapPort";
    public static final String CONF_SNMP_VERSION = "snmpVersion";
    public static final String CONF_SNMP_TRAP_VERSION = "snmpTrapVersion";
    public static final String CONF_AUTH = "authenticationType";
    public static final String CONF_USERNAME = "username";
    public static final String CONF_PASSWD = "password";
    public static final String CONF_ENCRYPTION = "encryptionType";
    public static final String CONF_COMMUNITY = "community";

    public static final String DEFAULT_ADDRESS = "localhost";
    public static final Integer DEFAULT_SNMP_PORT = 161;
    public static final Integer DEFAULT_TRAP_PORT = 162;
    public static final String DEFAULT_SNMP_VERSION = "V1";
    public static final String DEFAULT_SNMP_TRAP_VERSION = "V1";
    public static final String DEFAULT_ENCRYPTION = "MD5";
    public static final String DEFAULT_AUTH = "NOAUTH_NOPRIV";
}
