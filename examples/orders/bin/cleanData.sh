#!/bin/bash

cd "$(dirname $0)/../"

rm -rf data/agent1/channel/avro/*
rm -rf data/agent1/channel/cassandra/*
rm -rf data/agent1/channel/elastic/*
rm -rf data/agent1/channel/file/*
rm -rf data/agent1/channel/hdfs/*
rm -rf data/agent1/channel/kafka/*
rm -rf data/agent1/channel/streaming/*
rm -rf data/agent1/sink/avro/*
rm -rf data/agent1/sink/cassandra/*
rm -rf data/agent1/sink/elastic/*
rm -rf data/agent1/sink/file/*
rm -rf data/agent1/sink/hdfs/*
rm -rf data/agent1/sink/kafka/*
rm -rf data/agent1/sink/streaming/*

rm -f data/input/*
rm -f data/input/.*
