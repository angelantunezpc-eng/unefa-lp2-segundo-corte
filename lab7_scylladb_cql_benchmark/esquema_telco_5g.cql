CREATE KEYSPACE IF NOT EXISTS unefa_telco_5g
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'replication_factor': 3
};

CREATE TABLE IF NOT EXISTS unefa_telco_5g.cdr_por_abonado_dia (
    msisdn text,
    bucket_fecha text,
    timestamp_evento timestamp,
    id_cdr uuid,
    celda_5g text,
    bytes_consumidos bigint,
    PRIMARY KEY ((msisdn, bucket_fecha), timestamp_evento, id_cdr)
) WITH CLUSTERING ORDER BY (timestamp_evento DESC, id_cdr ASC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
  }
  AND default_time_to_live = 2592000;
