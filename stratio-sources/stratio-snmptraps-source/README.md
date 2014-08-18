Stratio SNMP Traps Source
==============================

A Flume source that listens to snmp traps.

Configuration
=============

Available config parameters:

- `address` *(string)*: Address to listen snmp traps. Default: localhost
- `snmpTrapPort` *(integer)*: Port to listen snmp traps. Default: 162
- `snmpVersion` *(string)*: SNMP Protocol version. Possible values: V1,V2c,V3. Default: V1
- `snmpTrapVersion`*(string)* : SNMP Trap Protocol version. Possible values: V1,V2c,V3. Default: V1
- `encryptionType` *(string)*: Encryption. Possible values: SHA, MD5. Default: MD5
- `authenticationType` *(string)*: SNMP Authentication. Possible values: AUTH_NOPRIV, NOAUTH_NOPRIV, AUTH_PRIV. Default: NOAUTH_NOPRIV
- `username` *(string)*: username. Required when authenticationType -> AUTH_NOPRIV, AUTH_PRIV
- `password` *(string)*: password. Required When authenticationType -> AUTH_NOPRIV, AUTH_PRIV
- `privacyProtocol` *(string)*: Privacy protocol. Required when authenticationType -> AUTH_PRIV. Possible values: PrivDES, Priv3DES, PrivAES128, PrivAES192, PrivAES256, PrivAES192With3DESKeyExtension, PrivAES256With3DESKeyExtension. Default: PRIVDES. 
- `privacyPassphrase` *(string)*: Privacy passphrase. Required when authenticationType -> AUTH_PRIV.


Sample Complete-flow Flume config
=================================

The following paragraph describes an example configuration of an Flume agent that uses our SNMPTraps source, a [File channel](http://flume.apache.org/FlumeUserGuide.html#file-channel) and print events in [Logger Sink](https://flume.apache.org/FlumeUserGuide.html#logger-sink).

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
agent.channels.c1.type = file
agent.channels.c1.checkpointDir = /home/user/flume/channel/check/
agent.channels.c1.dataDirs = /home/user/flume/channel/data/
agent.channels.c1.transactionCapacity=10000


# Bind the source and sink to the channel
agent.sources.snmp.channels = c1
agent.sinks.logSink.channel = c1
```
