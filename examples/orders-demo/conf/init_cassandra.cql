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
  
  
