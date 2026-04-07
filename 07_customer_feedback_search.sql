-- ============================================================
-- STEP 7: CORTEX SEARCH SERVICE FOR CUSTOMER FEEDBACK
-- Run as: ACCOUNTADMIN
-- Creates the CUSTOMER_FEEDBACK_TOOL Cortex Search Service
-- over the CUSTOMER_EXPERIENCE_COMMENTS table so the agent
-- can perform semantic search on product reviews and feedback.
--
-- Service: CUSTOMER_FEEDBACK_TOOL
--   - Source: CUSTOMER_EXPERIENCE_COMMENTS (52 rows)
--   - Search column: COMMENT_TEXT
--   - Filter attributes: COMMENT_ID, COMMENT_DATE, ARTICLE_ID, ARTICLE_NAME
--   - Embedding model: snowflake-arctic-embed-l-v2.0
--   - Refresh lag: 1 day
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- Create CUSTOMER_FEEDBACK_TOOL Cortex Search Service
-- ============================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE CUSTOMER_FEEDBACK_TOOL
    ON COMMENT_TEXT
    ATTRIBUTES COMMENT_ID, COMMENT_DATE, ARTICLE_ID, ARTICLE_NAME
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
    SELECT
        COMMENT_ID,
        COMMENT_DATE,
        ARTICLE_ID,
        ARTICLE_NAME,
        COMMENT_TEXT
    FROM CUSTOMER_EXPERIENCE_COMMENTS
);

-- ============================================================
-- Grant usage to both roles
-- ============================================================
GRANT USAGE ON CORTEX SEARCH SERVICE CUSTOMER_FEEDBACK_TOOL TO ROLE BIKE_ROLE;
GRANT USAGE ON CORTEX SEARCH SERVICE CUSTOMER_FEEDBACK_TOOL TO ROLE SNOW_ROLE;

-- ============================================================
-- Verification
-- ============================================================
SHOW CORTEX SEARCH SERVICES IN SCHEMA CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC;

-- Preview: bike query
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.CUSTOMER_FEEDBACK_TOOL',
        '{
            "query": "bike frame quality and warranty issues",
            "columns": ["COMMENT_TEXT", "ARTICLE_NAME", "COMMENT_DATE"],
            "limit": 3
        }'
    )
)['results'] AS bike_feedback_results;

-- Preview: snow query
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.CUSTOMER_FEEDBACK_TOOL',
        '{
            "query": "ski performance and quality",
            "columns": ["COMMENT_TEXT", "ARTICLE_NAME", "COMMENT_DATE"],
            "limit": 3
        }'
    )
)['results'] AS ski_feedback_results;
