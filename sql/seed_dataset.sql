CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.dataset_rows;
CREATE TABLE raw.dataset_rows (
  row_id      BIGSERIAL PRIMARY KEY,
  dataset     TEXT NOT NULL DEFAULT 'demo_llm',
  split       TEXT NOT NULL,           -- train / val / test
  text        TEXT NOT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- exact duplicates (same text)
INSERT INTO raw.dataset_rows (split, text) VALUES
('train', 'Explain what a primary key is.'),
('train', 'Explain what a primary key is.'),

-- near duplicates (not caught in v1, but useful later)
('train', 'Explain what a primary key is in databases.'),
('val',   'Explain what a primary key is.'),  -- leakage across splits (exact)

-- more examples
('train', 'Write a SQL query to count orders by city.'),
('test',  'Write a SQL query to count orders by city.'), -- leakage across splits (exact)
('test',  'What is CDC in data engineering?');