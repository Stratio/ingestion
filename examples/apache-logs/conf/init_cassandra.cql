
CREATE KEYSPACE IF NOT EXISTS test WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE test;

CREATE TABLE IF NOT EXISTS access_logs (
  log_id uuid,
  data text,
  log_bytes_returned text,
  log_date text,
  log_host text,
  log_http_code int,
  log_http_method text,
  log_http_version text,
  log_url_path text,
  log_user text,
  PRIMARY KEY ((log_id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};


