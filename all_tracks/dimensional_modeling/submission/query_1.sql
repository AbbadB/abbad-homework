CREATE TABLE abbad.actors (
  actor VARCHAR,
  actorid VARCHAR,
  films ARRAY (
    ROW (
      film VARCHAR,
      votes INT,
      rating DOUBLE,
      filmid VARCHAR
    )
  ),
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['quality_class']
)