INSERT INTO data_file_type (id) VALUES
  ('ETAgeCalc'),
  ('NuAgeCalc'),
  ('Sample metadata')
ON CONFLICT DO NOTHING;

/* A cached CSV copy of each data table. The pipeline reads workbooks directly
  and no longer fills this in; the column stays for the tables already stored. */
ALTER TABLE data_file ADD COLUMN csv_data bytea;

-- Embargo permanantly by default
ALTER TABLE project ALTER COLUMN embargo_date SET DEFAULT 'infinity';

CREATE SCHEMA lab_view;
CREATE OR REPLACE VIEW lab_view.aggregate_histogram AS
WITH a AS (
SELECT
	d.id,
	value
FROM datum d
JOIN datum_type dt
  ON d.type = dt.id
WHERE d.is_accepted
  AND dt.unit = 'Ma'
),
b AS (
SELECT
 width_bucket(value, 0, 4500, 900) AS bucket,
 count(*)
FROM a
GROUP BY bucket
ORDER BY bucket
)
SELECT
	(bucket-1)*5 min_age,
	bucket*5 max_age,
	count
FROM b;
