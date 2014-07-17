Stratio SNMP Traps Source
==============================

A Flume source that listens to snmp traps.

Available config parameters:

- address : Address to listen snmp traps. Default: localhost
- snmpTrapPort : Port to listen snmp traps. Default: 162
- snmpVersion : SNMP Protocol version. Possible values: V1,V2c,V3. Default: V1
- snmpTrapVersion : SNMP Trap Protocol version. Possible values: V1,V2c,V3. Default: V1
- authenticationType : SNMP Authentication. Possible values: AUTH_NOPRIV, NOAUTH_NOPRIV, AUTH_PRIV. Default: NOAUTH_NOPRIV
- encryptionType : Encryption. Possible values: SHA, MD5. Default: MD5
- username : username if neccesary.
- password : password if neccesary.


Sample Flume config
-------------------

