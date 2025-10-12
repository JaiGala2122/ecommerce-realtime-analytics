CREATE TABLE IF NOT EXISTS metrics_minute (
  window_start TIMESTAMP NOT NULL,
  window_end   TIMESTAMP NOT NULL,
  country      TEXT,
  transaction_count BIGINT,
  total_revenue NUMERIC(18,2),
  avg_revenue   NUMERIC(18,4),
  PRIMARY KEY (window_start, country)
);
