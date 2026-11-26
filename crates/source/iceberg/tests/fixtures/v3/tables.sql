CREATE NAMESPACE IF NOT EXISTS fx.db;

CREATE TABLE fx.db.dv (id BIGINT, name STRING) USING iceberg TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read');
INSERT INTO fx.db.dv VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO fx.db.dv VALUES (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h');
DELETE FROM fx.db.dv WHERE id IN (2, 7);
DELETE FROM fx.db.dv WHERE id = 3;

CREATE TABLE fx.db.variant (id BIGINT, payload VARIANT) USING iceberg TBLPROPERTIES ('format-version' = '3');
INSERT INTO fx.db.variant
  SELECT 1, parse_json('{"a": 1, "b": [true, null, "x"]}')
  UNION ALL SELECT 2, parse_json('42')
  UNION ALL SELECT 3, parse_json('"gone"');
DELETE FROM fx.db.variant WHERE id = 3;

CREATE TABLE fx.db.upgraded (id BIGINT, name STRING) USING iceberg TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read');
INSERT INTO fx.db.upgraded VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO fx.db.upgraded VALUES (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h');
DELETE FROM fx.db.upgraded WHERE id = 2;
DELETE FROM fx.db.upgraded WHERE id = 6;
ALTER TABLE fx.db.upgraded SET TBLPROPERTIES ('format-version' = '3');
DELETE FROM fx.db.upgraded WHERE id = 3;

SELECT 'dv' AS t, id, name FROM fx.db.dv ORDER BY id;
SELECT content, file_path, file_format, record_count FROM fx.db.dv.all_files ORDER BY content, file_path;
SELECT 'variant' AS t, id, to_json(payload) FROM fx.db.variant ORDER BY id;
SELECT content, file_path, file_format, record_count FROM fx.db.variant.files ORDER BY content, file_path;
SELECT 'upgraded' AS t, id, name FROM fx.db.upgraded ORDER BY id;
SELECT content, file_path, file_format, record_count FROM fx.db.upgraded.files ORDER BY content, file_path;
