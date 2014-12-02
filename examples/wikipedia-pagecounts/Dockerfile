FROM centos:centos6 
MAINTAINER Santiago M. Mola <smola@stratio.com>

ADD . /demo

RUN echo "[stratio]" > /etc/yum.repos.d/stratio.repo &&\
	echo "name=Stratio Package Repository" >> /etc/yum.repos.d/stratio.repo &&\
	echo "baseurl=http://prerepository.stratio.com/TEST/1.0.0/RHEL/6.5" >> /etc/yum.repos.d/stratio.repo &&\
	echo "gpgcheck=0" >> /etc/yum.repos.d/stratio.repo &&\
	echo "enabled=1" >> /etc/yum.repos.d/stratio.repo

RUN yum install -y java-1.7.0-openjdk-devel
RUN yum install -y stratio-ingestion

RUN sed -i -e "s~127.0.0.1~cassandra~g" /demo/conf/flume-conf-cassandra.properties
RUN sed -i -e "s~127.0.0.1~mongodb~g" /demo/conf/flume-conf-mongodb.properties
RUN sed -i -e "s~127.0.0.1~elasticsearch~g" /demo/conf/flume-conf-elasticsearch.properties

CMD ["/bin/bash"]
