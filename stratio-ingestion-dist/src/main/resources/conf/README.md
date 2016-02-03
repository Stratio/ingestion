How to configure ingestion-initscript
=================================

This file can be used as reference to configure multiple Ingestion agents that works related. Some times it's needed to 
configure agents which depends of other agents. In order to start & stop Ingestion agents working in this way, this file
is a small guide to use the provided init scripts.

# First of all we want to add the following ingestion agents as a service

for i in agent1 agent2 agent3 agent4 ; do
    ln -sf /opt/sds/ingestion/bin/ingestion-init /etc/init.d/ingestion-$i
    chkconfig --add ingestion-$i
    chkconfig ingestion-$i off
done
chkconfig ingestion-agent1 on

# Check kill links on /etc/rc.d/rc*.d
[root@mmdevapp ~]# find /etc/ -iname K\*ingestion-\* | cut -d '/' -f1-4 | sort -u
/etc/rc.d/rc0.d
/etc/rc.d/rc1.d
/etc/rc.d/rc2.d
/etc/rc.d/rc3.d
/etc/rc.d/rc4.d
/etc/rc.d/rc5.d
/etc/rc.d/rc6.d

# Check start links on /etc/rc.d/rc*.d

[root@mmdevapp ~]# find /etc/ -iname S\*ingestion-\* | cut -d '/' -f1-4 | sort -u
/etc/rc.d/rc2.d
/etc/rc.d/rc3.d
/etc/rc.d/rc4.d
/etc/rc.d/rc5.d
