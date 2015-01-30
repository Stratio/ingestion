CREATE KEYSPACE IF NOT EXISTS flume_wikipedia_demo WITH REPLICATION =  { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE TABLE IF NOT EXISTS flume_wikipedia_demo.flume_wikipedia_demo (proj_lang text, title text, reqs int, "timestamp" timestamp, PRIMARY KEY ((proj_lang, title), timestamp));
