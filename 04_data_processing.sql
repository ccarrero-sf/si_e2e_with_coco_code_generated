-- ============================================================
-- STEP 4: AI DATA PROCESSING - PDF & IMAGE PIPELINE
-- Run as: ACCOUNTADMIN
-- Processes PDFs and images from the doc stages into a single
-- DOCS_CHUNKS_TABLE table, then runs sanity checks.
--
-- Pipeline:
--   PDF  -> AI_PARSE_DOCUMENT (LAYOUT) -> SPLIT_TEXT_RECURSIVE_CHARACTER
--        -> AI_CLASSIFY (filename + first 500 chars) -> DOCS_CHUNKS_TABLE
--   IMG  -> AI_COMPLETE claude-3-7-sonnet (image description)
--        -> AI_CLASSIFY (filename) -> DOCS_CHUNKS_TABLE (one row/image)
--
-- Chunk settings: chunk_size=1500, overlap=100, format='markdown'
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- PART A: Enable directory tables on doc stages
-- (Required for DIRECTORY() table function and AI image functions)
-- ============================================================
ALTER STAGE BIKE_DOCS_STAGE SET DIRECTORY = (ENABLE = TRUE);
ALTER STAGE SNOW_DOCS_STAGE SET DIRECTORY = (ENABLE = TRUE);

-- Refresh directory metadata so newly copied files are visible
ALTER STAGE BIKE_DOCS_STAGE REFRESH;
ALTER STAGE SNOW_DOCS_STAGE REFRESH;

-- Verify directory is populated
SELECT 'BIKE_DOCS_STAGE' AS stage, COUNT(*) AS file_count FROM DIRECTORY(@BIKE_DOCS_STAGE)
UNION ALL
SELECT 'SNOW_DOCS_STAGE', COUNT(*) FROM DIRECTORY(@SNOW_DOCS_STAGE);

-- ============================================================
-- PART B: Create destination table
-- ============================================================
CREATE OR REPLACE TABLE DOCS_CHUNKS_TABLE (
    CHUNK_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    SOURCE_FILE      VARCHAR NOT NULL,
    FILE_TYPE        VARCHAR NOT NULL,   -- 'PDF' | 'IMAGE'
    PRODUCT_CATEGORY VARCHAR NOT NULL,   -- 'BIKE' | 'SNOW'  (AI-classified)
    STAGE_NAME       VARCHAR NOT NULL,   -- 'BIKE_DOCS_STAGE' | 'SNOW_DOCS_STAGE'
    CHUNK_TEXT       VARCHAR NOT NULL,   -- text chunk (PDF) or image description (IMAGE)
    CHUNK_INDEX      NUMBER  NOT NULL    -- 0-based position within source file
);

-- ============================================================
-- PART C: Process PDF files from BIKE_DOCS_STAGE
-- ============================================================

-- Step C1: Parse all bike PDFs (AI_PARSE_DOCUMENT extracts full text as markdown)
CREATE OR REPLACE TEMP TABLE BIKE_PDF_PARSED AS
SELECT
    RELATIVE_PATH,
    AI_PARSE_DOCUMENT(
        TO_FILE('@BIKE_DOCS_STAGE', RELATIVE_PATH),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS doc_text
FROM DIRECTORY(@BIKE_DOCS_STAGE)
WHERE RELATIVE_PATH ILIKE '%.pdf';

-- Step C2: Classify each bike PDF once (filename + first 500 chars of text)
CREATE OR REPLACE TEMP TABLE BIKE_PDF_CLASSIFIED AS
SELECT
    RELATIVE_PATH,
    doc_text,
    AI_CLASSIFY(
        RELATIVE_PATH || '. ' || LEFT(doc_text, 500),
        ['BIKE', 'SNOW']
    ):labels[0]::VARCHAR AS product_category
FROM BIKE_PDF_PARSED
WHERE doc_text IS NOT NULL;

-- Step C3: Chunk and insert bike PDF content
INSERT INTO DOCS_CHUNKS_TABLE (SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, STAGE_NAME, CHUNK_TEXT, CHUNK_INDEX)
SELECT
    RELATIVE_PATH          AS source_file,
    'PDF'                  AS file_type,
    product_category,
    'BIKE_DOCS_STAGE'      AS stage_name,
    c.value::VARCHAR       AS chunk_text,
    c.index::NUMBER        AS chunk_index
FROM BIKE_PDF_CLASSIFIED,
LATERAL FLATTEN(
    input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(doc_text, 'markdown', 1500, 100)
) c
WHERE c.value::VARCHAR IS NOT NULL
  AND LENGTH(c.value::VARCHAR) > 0;

-- ============================================================
-- PART D: Process PDF files from SNOW_DOCS_STAGE
-- ============================================================

-- Step D1: Parse all snow PDFs
CREATE OR REPLACE TEMP TABLE SNOW_PDF_PARSED AS
SELECT
    RELATIVE_PATH,
    AI_PARSE_DOCUMENT(
        TO_FILE('@SNOW_DOCS_STAGE', RELATIVE_PATH),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS doc_text
FROM DIRECTORY(@SNOW_DOCS_STAGE)
WHERE RELATIVE_PATH ILIKE '%.pdf';

-- Step D2: Classify each snow PDF once
CREATE OR REPLACE TEMP TABLE SNOW_PDF_CLASSIFIED AS
SELECT
    RELATIVE_PATH,
    doc_text,
    AI_CLASSIFY(
        RELATIVE_PATH || '. ' || LEFT(doc_text, 500),
        ['BIKE', 'SNOW']
    ):labels[0]::VARCHAR AS product_category
FROM SNOW_PDF_PARSED
WHERE doc_text IS NOT NULL;

-- Step D3: Chunk and insert snow PDF content
INSERT INTO DOCS_CHUNKS_TABLE (SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, STAGE_NAME, CHUNK_TEXT, CHUNK_INDEX)
SELECT
    RELATIVE_PATH          AS source_file,
    'PDF'                  AS file_type,
    product_category,
    'SNOW_DOCS_STAGE'      AS stage_name,
    c.value::VARCHAR       AS chunk_text,
    c.index::NUMBER        AS chunk_index
FROM SNOW_PDF_CLASSIFIED,
LATERAL FLATTEN(
    input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(doc_text, 'markdown', 1500, 100)
) c
WHERE c.value::VARCHAR IS NOT NULL
  AND LENGTH(c.value::VARCHAR) > 0;

-- ============================================================
-- PART E: Process IMAGE files from BIKE_DOCS_STAGE
-- AI_COMPLETE generates a rich description; AI_CLASSIFY assigns category
-- NOTE: If claude-3-7-sonnet is unavailable for images, use claude-3-5-sonnet
-- ============================================================
INSERT INTO DOCS_CHUNKS_TABLE (SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, STAGE_NAME, CHUNK_TEXT, CHUNK_INDEX)
SELECT
    RELATIVE_PATH AS source_file,
    'IMAGE'       AS file_type,
    AI_CLASSIFY(
        RELATIVE_PATH,
        ['BIKE', 'SNOW']
    ):labels[0]::VARCHAR AS product_category,
    'BIKE_DOCS_STAGE' AS stage_name,
    AI_COMPLETE(
        'claude-3-7-sonnet',
        PROMPT('Provide a detailed description of this product image {0}. Focus on the product shown, its visible features, design characteristics, colors, components, and any relevant details useful for a customer considering this product.',
            TO_FILE('@BIKE_DOCS_STAGE', RELATIVE_PATH))
    ) AS chunk_text,
    0 AS chunk_index
FROM DIRECTORY(@BIKE_DOCS_STAGE)
WHERE RELATIVE_PATH ILIKE '%.jpeg'
   OR RELATIVE_PATH ILIKE '%.jpg'
   OR RELATIVE_PATH ILIKE '%.png';

-- ============================================================
-- PART F: Process IMAGE files from SNOW_DOCS_STAGE
-- ============================================================
INSERT INTO DOCS_CHUNKS_TABLE (SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, STAGE_NAME, CHUNK_TEXT, CHUNK_INDEX)
SELECT
    RELATIVE_PATH AS source_file,
    'IMAGE'       AS file_type,
    AI_CLASSIFY(
        RELATIVE_PATH,
        ['BIKE', 'SNOW']
    ):labels[0]::VARCHAR AS product_category,
    'SNOW_DOCS_STAGE' AS stage_name,
    AI_COMPLETE(
        'claude-3-7-sonnet',
        PROMPT('Provide a detailed description of this product image {0}. Focus on the product shown, its visible features, design characteristics, colors, components, and any relevant details useful for a customer considering this product.',
            TO_FILE('@SNOW_DOCS_STAGE', RELATIVE_PATH))
    ) AS chunk_text,
    0 AS chunk_index
FROM DIRECTORY(@SNOW_DOCS_STAGE)
WHERE RELATIVE_PATH ILIKE '%.jpeg'
   OR RELATIVE_PATH ILIKE '%.jpg'
   OR RELATIVE_PATH ILIKE '%.png';

-- ============================================================
-- PART G: POST-INSERT SANITY CHECKS
-- ============================================================

-- Check 1: Overall row counts by file type and category
SELECT
    FILE_TYPE,
    PRODUCT_CATEGORY,
    COUNT(*)              AS chunk_count,
    COUNT(DISTINCT SOURCE_FILE) AS file_count
FROM DOCS_CHUNKS_TABLE
GROUP BY FILE_TYPE, PRODUCT_CATEGORY
ORDER BY FILE_TYPE, PRODUCT_CATEGORY;

-- Check 2: NULL values (expect 0 for both)
SELECT
    COUNT_IF(CHUNK_TEXT IS NULL)       AS null_chunk_text,
    COUNT_IF(PRODUCT_CATEGORY IS NULL) AS null_category,
    COUNT_IF(LENGTH(CHUNK_TEXT) = 0)   AS empty_chunk_text
FROM DOCS_CHUNKS_TABLE;

-- Check 3: Per-file chunk counts (PDFs should have multiple chunks; images = 1)
SELECT
    SOURCE_FILE,
    FILE_TYPE,
    PRODUCT_CATEGORY,
    COUNT(*) AS chunks
FROM DOCS_CHUNKS_TABLE
GROUP BY SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY
ORDER BY FILE_TYPE, SOURCE_FILE;

-- Check 4: Category distribution matches expectations
-- BIKE files: Mondracer, Premium_Bicycle, Xtreme, Ultimate_Downhill -> BIKE
-- SNOW files: Carver, OutPiste, RacingFast, Ski_Boots, Outpiste -> SNOW
SELECT
    SOURCE_FILE,
    PRODUCT_CATEGORY,
    CASE
        WHEN SOURCE_FILE ILIKE '%bike%'
          OR SOURCE_FILE ILIKE '%bicycle%'
          OR SOURCE_FILE ILIKE '%mondracer%'
          OR SOURCE_FILE ILIKE '%downhill%'
          OR SOURCE_FILE ILIKE '%xtreme%'
          OR SOURCE_FILE ILIKE '%road%'
        THEN 'BIKE'
        ELSE 'SNOW'
    END AS expected_category,
    IFF(PRODUCT_CATEGORY = CASE
        WHEN SOURCE_FILE ILIKE '%bike%'
          OR SOURCE_FILE ILIKE '%bicycle%'
          OR SOURCE_FILE ILIKE '%mondracer%'
          OR SOURCE_FILE ILIKE '%downhill%'
          OR SOURCE_FILE ILIKE '%xtreme%'
          OR SOURCE_FILE ILIKE '%road%'
        THEN 'BIKE'
        ELSE 'SNOW'
    END, 'CORRECT', 'MISMATCH') AS classification_result
FROM DOCS_CHUNKS_TABLE
GROUP BY SOURCE_FILE, PRODUCT_CATEGORY
ORDER BY SOURCE_FILE;
