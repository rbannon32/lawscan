-- Per-part daily rollup (deterministic content hash by ordered child hashes)
CREATE OR REPLACE TABLE ecfr.parts_daily AS
SELECT
  version_date,
  ANY_VALUE(snapshot_ts) AS snapshot_ts,
  title_num, ANY_VALUE(title_name) AS title_name,
  ANY_VALUE(chapter_label) AS chapter_label,
  ANY_VALUE(subchapter_label) AS subchapter_label,
  part_num, ANY_VALUE(part_label) AS part_label,
  ANY_VALUE(agency_name) AS agency_name,
  SUM(word_count) AS part_word_count,
  TO_HEX(SHA256(STRING_AGG(section_hash, '' ORDER BY section_citation))) AS part_hash
FROM ecfr.sections
GROUP BY version_date, title_num, part_num;

-- Per-agency daily rollup
CREATE OR REPLACE TABLE ecfr.agency_daily AS
SELECT
  version_date,
  agency_name,
  ANY_VALUE(snapshot_ts) AS snapshot_ts,
  SUM(part_word_count) AS agency_word_count,
  TO_HEX(SHA256(STRING_AGG(part_hash, '' ORDER BY title_num, part_num))) AS agency_hash
FROM ecfr.parts_daily
GROUP BY version_date, agency_name;
