-- Gold table: one row per country per hour of invoice time, upserted by spark_job.py
CREATE TABLE IF NOT EXISTS sales_by_country_hour (
  window_start      TIMESTAMP     NOT NULL,
  window_end        TIMESTAMP     NOT NULL,
  country           TEXT          NOT NULL,
  order_lines       BIGINT        NOT NULL,
  invoices          BIGINT        NOT NULL,   -- approximate distinct count
  revenue           NUMERIC(14,2) NOT NULL,   -- sales lines only
  returns_value     NUMERIC(14,2) NOT NULL,   -- value of returned lines, positive
  last_ingest_time  TIMESTAMP     NOT NULL,   -- newest event folded into this row
  updated_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
  PRIMARY KEY (window_start, country)
);
