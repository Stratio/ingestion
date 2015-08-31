

CREATE TABLE orders (
  id UUID,
  timestamp text,
  client_id int,
  payment_method text,
  shopping_center text,
  city text,
  total_amount double,
  employee int,
  product text,
  family text,
  price double,
  quantitiy int,
  PRIMARY KEY (client_id, timestamp, id));

CREATE CUSTOM INDEX orders_idx ON orders (lucene) 
USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {       
'refresh_seconds':'60',        
'ram_buffer_mb':'64',       
'max_merge_mb':'16',       
'max_cached_mb':'64',       
'schema':'{fields:
  {timestamp:{type:"date", pattern:""yyyy/MM/dd HH:mm:ss"},
   client_id:{type:"integer"},
   payment_method:{type:"string"},
   shopping_center:{type:"string"},
   city:{type:"string"},
   total_amount:{type:"double"},
   employee:{type:"integer"},
   product:{type:"string"},
   family:{type:"string"},
   price:{type:"double"},
   quantitiy:{type:"integer"}
  }
}'};

CREATE TABLE aggregations (
  id UUID,
  aggregation_type text,
  day timestamp,
  client_id int,
  payment_method text,
  shopping_center text,
  city text,
  total_amount double,
  employee int,
  product text,
  family text,
  price double,
  quantitiy int,
  count double, sum double, max double, min double, average double,
  PRIMARY KEY (id));

CREATE CUSTOM INDEX aggregations_idx ON aggregations (lucene) 
USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {       
'refresh_seconds':'60',        
'ram_buffer_mb':'64',       
'max_merge_mb':'16',       
'max_cached_mb':'64',       
'schema':'{fields:
  {aggregation_type:{type:"string"},
   day:{type:"date", pattern:""yyyy/MM/dd"},
   client_id:{type:"integer"},
   payment_method:{type:"string"},
   shopping_center:{type:"string"},
   city:{type:"string"},
   total_amount:{type:"double"},
   employee:{type:"integer"},
   product:{type:"string"},
   family:{type:"string"},
   price:{type:"double"},
   quantitiy:{type:"integer"}
  }
}'};

SELECT count, sum, max, min, average, day FROM aggregations WHERE lucene='{
  filter:{type:"boolean", must:[
          {type:"match", field:"aggregation_type" ,value:"sales_per_product_and_day"},
          {type:"match", field:"product" ,value:"Apples"},
          {type:"range", field:"day" ,lower:"15/01/01", upper:"15/01/15"}
         ]}
}'















