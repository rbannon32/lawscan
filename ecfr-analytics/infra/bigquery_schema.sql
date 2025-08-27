-- BigQuery DDL — section-grain fact table + optional rollups
CREATE SCHEMA IF NOT EXISTS ecfr;

CREATE TABLE IF NOT EXISTS ecfr.sections (
  version_date     DATE NOT NULL,
  snapshot_ts      TIMESTAMP NOT NULL,

  title_num        INT64 NOT NULL,
  title_name       STRING,
  chapter_id       STRING,
  chapter_label    STRING,
  subchapter_id    STRING,
  subchapter_label STRING,
  part_num         STRING,
  part_label       STRING,
  subpart_id       STRING,
  subpart_label    STRING,
  section_num      STRING,
  section_citation STRING,
  section_heading  STRING,
  section_text     STRING,
  reserved         BOOL,

  agency_name      STRING,

  references       ARRAY<STRING>,
  authority_uscode ARRAY<STRING>,

  part_order       INT64,
  section_order    INT64,

  word_count                   INT64,
  modal_obligation_terms_count INT64,
  crossref_density_per_1k      FLOAT64,

  section_hash     STRING,
  normalized_text  STRING,

  raw_json         JSON,
  
  -- Enhanced regulatory metrics
  prohibition_count INT64,
  requirement_count INT64,
  exception_count INT64,
  sentence_count INT64,
  avg_sentence_length FLOAT64,
  dollar_mentions INT64,
  temporal_references INT64,
  enforcement_terms INT64,
  regulatory_burden_score FLOAT64,
  
  -- AI-optimized fields for RAG and embeddings
  ai_context_summary STRING,
  embedding_optimized_text STRING
)
PARTITION BY version_date
CLUSTER BY title_num, agency_name, part_num;
