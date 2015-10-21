CREATE KEYSPACE IF NOT EXISTS orders WITH REPLICATION =  {
 'class' : 'SimpleStrategy', 'replication_factor' : 1
 };


CREATE TABLE IF NOT EXISTS orders.orders (
  order_id UUID,
  timestamp text,
  client_id int,
  payment_method text,
  latitude double,
  longitude double,
  credit_card text,
  shopping_center text,
  channel text,
  city text,
  employee int,
  total_amount double,
  PRIMARY KEY (order_id));
