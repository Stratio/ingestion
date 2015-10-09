CREATE KEYSPACE IF NOT EXISTS demoCassandra WITH REPLICATION =  { 'class' : 'SimpleStrategy', 'replication_factor' :
1 };
CREATE TABLE IF NOT EXISTS demoCassandra.demoCassandra (name text, age text,
 PRIMARY KEY ((name, age)));
