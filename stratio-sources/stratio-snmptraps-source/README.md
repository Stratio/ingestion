Stratio SNMP Traps Source
==============================

A Flume source that listens to snmp traps.

Available config parameters:

- address : Address to listen snmp traps. Default: localhost
- snmpTrapPort : Port to listen snmp traps. Default: 162
- snmpVersion : SNMP Protocol version. Possible values: V1,V2c,V3. Default: V1
- snmpTrapVersion : SNMP Trap Protocol version. Possible values: V1,V2c,V3. Default: V1
- encryptionType : Encryption. Possible values: SHA, MD5. Default: MD5
- authenticationType : SNMP Authentication. Possible values: AUTH_NOPRIV, NOAUTH_NOPRIV, AUTH_PRIV. Default: NOAUTH_NOPRIV
- username : username. Required when authenticationType -> AUTH_NOPRIV, AUTH_PRIV
- password : password. Required When authenticationType -> AUTH_NOPRIV, AUTH_PRIV
- privacyProtocol : Privacy protocol. Required when authenticationType -> AUTH_PRIV. Possible values: PrivDES, Priv3DES, PrivAES128, PrivAES192, PrivAES256, PrivAES192With3DESKeyExtension, PrivAES256With3DESKeyExtension. Default: PRIVDES. 
- privacyPassphrase : Privacy passphrase. Required when authenticationType -> AUTH_PRIV.


Sample Flume config
-------------------

```
# Name the components on this agent
agent.sources = snmp
agent.sinks = logSink
agent.channels = c1

# Describe the source
agent.sources.snmp.type=com.stratio.ingestion.source.snmptraps.SNMPSource
agent.sources.snmp.address=localhost
agent.sources.snmp.snmpTrapPort=162
agent.sources.snmp.snmpVersion=v2c
agent.sources.snmp.encryptionType=MD5
agent.sources.snmp.authenticationType=AUTH_PRIV
agent.sources.snmp.username=user
agent.sources.snmp.password=12345678
agent.sources.snmp.privacyProtocol=Priv3DES
agent.sources.snmp.privacyPassphrase=mypassphrase


# Describe the sink
agent.sinks.logSink.type = logger

# Use a channel which buffers events in file
agent.channels.c1.type = memory 

# Bind the source and sink to the channel
agent.sources.snmp.channels = c1
agent.sinks.logSink.channel = c1
```
