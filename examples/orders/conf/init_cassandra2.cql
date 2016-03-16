CREATE KEYSPACE IF NOT EXISTS ingestion WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

USE ingestion;

CREATE TABLE IF NOT EXISTS lines (
  line_id UUID,
  order_id UUID,
  timestamp text,
  day_time_zone text,
  client_id int,
  payment_method text,
  latitude double,
  longitude double,  
  credit_card bigint,
  shopping_center text,
  channel text,
  city text,
  country text,
  employee int,
  total_amount double,  
  total_products int,
  order_size text,  
  product text,
  family text,
  price double,
  quantitiy int,
  lucene text,
  PRIMARY KEY (line_id));

CREATE CUSTOM INDEX IF NOT EXISTS lines_idx ON lines (lucene) 
USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {       
'refresh_seconds':'60',        
'ram_buffer_mb':'64',       
'max_merge_mb':'16',       
'max_cached_mb':'64',       
'schema':'{fields:
  {timestamp:{type:"date", pattern:"yyyy/MM/dd HH:mm:ss"},
   day_time_zone:{type:"string"},
   client_id:{type:"integer"},
   payment_method:{type:"string"},
   credit_card:{type:"string"},
   shopping_center:{type:"string"},
   channel:{type:"string"},
   city:{type:"string"},
   country:{type:"string"},
   total_amount:{type:"double"},
   total_products:{type:"integer"},
   order_size:{type:"string"},
   employee:{type:"integer"},
   product:{type:"string"},
   family:{type:"string"},
   price:{type:"double"},
   quantitiy:{type:"integer"}
  }
}'};


