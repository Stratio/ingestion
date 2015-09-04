CREATE KEYSPACE IF NOT EXISTS ingestion WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

USE ingestion;

CREATE TABLE IF NOT EXISTS orders (
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
  PRIMARY KEY (client_id, timestamp, order_id));
  

ALTER TABLE orders ADD lucene text;
CREATE CUSTOM INDEX orders_idx ON orders (lucene) 
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
   point:{type:"geo_point", latitude:"latitude", longitude:"longitude"},
   credit_card:{type:"string"},
   shopping_center:{type:"string"},
   channel:{type:"string"},
   city:{type:"string"},
   country:{type:"string"},
   total_amount:{type:"double"},
   total_products:{type:"integer"},
   order_size:{type:"string"},
   employee:{type:"integer"}
  }
}'};


