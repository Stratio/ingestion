FROM stratio/ingestion:latest
MAINTAINER Santiago M. Mola <smola@stratio.com>

ADD . /demo

RUN sed -i -e "s~127.0.0.1~cassandra~g" /demo/conf/flume-conf-cassandra.properties
RUN sed -i -e "s~127.0.0.1~mongodb~g" /demo/conf/flume-conf-mongodb.properties
RUN sed -i -e "s~127.0.0.1~elasticsearch~g" /demo/conf/flume-conf-elasticsearch.properties

CMD ["/bin/bash"]
