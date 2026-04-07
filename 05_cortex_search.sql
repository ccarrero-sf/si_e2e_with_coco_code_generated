-- ============================================================
-- STEP 5: CORTEX SEARCH SERVICE
-- Run as: ACCOUNTADMIN
-- Creates the DOCUMENTATION_TOOL Cortex Search Service covering
-- all PDF chunks and image descriptions from DOCS_CHUNKS_TABLE.
--
-- Service:  DOCUMENTATION_TOOL
--   - All PDF and image content (both BIKE and SNOW)
--   - Embedding model: snowflake-arctic-embed-l-v2.0
--   - Refresh lag: 1 day
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- Create DOCUMENTATION_TOOL Cortex Search Service
-- Covers all chunks (PDF + IMAGE, BIKE + SNOW)
-- ============================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE DOCUMENTATION_TOOL
    ON CHUNK_TEXT
    ATTRIBUTES SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, CHUNK_INDEX, STAGE_NAME
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
    SELECT
        CHUNK_TEXT,
        SOURCE_FILE,
        FILE_TYPE,
        PRODUCT_CATEGORY,
        CHUNK_INDEX,
        STAGE_NAME
    FROM DOCS_CHUNKS_TABLE
);

-- ============================================================
-- Grant usage to both roles
-- ============================================================
GRANT USAGE ON CORTEX SEARCH SERVICE DOCUMENTATION_TOOL TO ROLE BIKE_ROLE;
GRANT USAGE ON CORTEX SEARCH SERVICE DOCUMENTATION_TOOL TO ROLE SNOW_ROLE;

-- ============================================================
-- Verification
-- ============================================================

-- Confirm service exists
SHOW CORTEX SEARCH SERVICES IN SCHEMA CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC;

-- Preview: bike query
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.DOCUMENTATION_TOOL',
        '{
            "query": "bike features and specifications",
            "columns": ["CHUNK_TEXT", "SOURCE_FILE", "FILE_TYPE", "PRODUCT_CATEGORY"],
            "limit": 3
        }'
    )
)['results'] AS bike_results;

-- Preview: snow query
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.DOCUMENTATION_TOOL',
        '{
            "query": "ski specifications and performance",
            "columns": ["CHUNK_TEXT", "SOURCE_FILE", "FILE_TYPE", "PRODUCT_CATEGORY"],
            "limit": 3
        }'
    )
)['results'] AS snow_results;
