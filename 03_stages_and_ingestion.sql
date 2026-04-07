-- ============================================================
-- STEP 3: STAGES AND DATA INGESTION
-- Run as: ACCOUNTADMIN
-- Creates three internal stages (all with Snowflake SSE),
-- grants role-based read access, then copies files from the
-- Git repository into the appropriate stages.
--
-- Stage layout:
--   CSV_STAGE       - structured CSV data  (BIKE_ROLE + SNOW_ROLE)
--   BIKE_DOCS_STAGE - bike PDFs / images   (BIKE_ROLE only)
--   SNOW_DOCS_STAGE - ski/snow PDFs / images (SNOW_ROLE only)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ---- Internal stages with Snowflake SSE encryption ----

CREATE OR REPLACE STAGE CSV_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT    = 'Structured CSV data files - accessible to both roles';

CREATE OR REPLACE STAGE BIKE_DOCS_STAGE
    DIRECTORY  = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT    = 'Bike product PDFs and images - BIKE_ROLE only';

CREATE OR REPLACE STAGE SNOW_DOCS_STAGE
    DIRECTORY  = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT    = 'Ski/snow product PDFs and images - SNOW_ROLE only';

-- ---- Stage access grants ----
-- CSV stage: both roles
GRANT READ ON STAGE CSV_STAGE       TO ROLE BIKE_ROLE;
GRANT READ ON STAGE CSV_STAGE       TO ROLE SNOW_ROLE;
-- Docs stages: role-specific
GRANT READ ON STAGE BIKE_DOCS_STAGE TO ROLE BIKE_ROLE;
GRANT READ ON STAGE SNOW_DOCS_STAGE TO ROLE SNOW_ROLE;

-- ============================================================
-- Copy CSV files from Git repository
-- ============================================================
COPY FILES INTO @CSV_STAGE
    FROM @SI_E2E_FILES_REPO/branches/main/csv/
    FILES = (
        'DIM_ARTICLE.csv',
        'DIM_CUSTOMER.csv',
        'eval_dataset.csv',
        'customer_experience_comments.csv_0_0_0.csv.gz',
        'fact_sales.csv_0_0_0.csv.gz'
    );

-- ============================================================
-- Copy bike-related documents from Git repository
-- ============================================================
COPY FILES INTO @BIKE_DOCS_STAGE
    FROM @SI_E2E_FILES_REPO/branches/main/docs/
    FILES = (
        'Mondracer Infant Bike.pdf',
        'Premium_Bicycle_User_Guide.pdf',
        'The Xtreme Road Bike 105 SL.pdf',
        'The_Ultimate_Downhill_Bike.pdf',
        'Premium_Bicycle_1.jpeg',
        'Premium_Bicycle_2.jpeg',
        'Premium_Bicycle_3.jpeg',
        'Premium_Bicycle_4.jpeg',
        'The_Ultimate_Downhill_Bike_1.jpeg',
        'The_Ultimate_Downhill_Bike_2.jpeg',
        'The_Xtreme_Road_Bike_3.jpeg',
        'The_Xtreme_Road_Bike_4.jpeg',
        'The_Xtreme_Road_Bike_5.jpeg'
    );

-- ============================================================
-- Copy ski/snow-related documents from Git repository
-- ============================================================
COPY FILES INTO @SNOW_DOCS_STAGE
    FROM @SI_E2E_FILES_REPO/branches/main/docs/
    FILES = (
        'Carver Skis Specification Guide.pdf',
        'OutPiste Skis Specification Guide.pdf',
        'RacingFast Skis Specification Guide.pdf',
        'Outpiste_Skis.jpeg',
        'Racing_Fast_Skis.jpeg',
        'Ski_Boots_TDBootz_Special.jpg',
        'Ski_Boots_TDBootz_Special.pdf'
    );

-- ---- Verification: list stage contents ----
LS @CSV_STAGE;        -- expect 5 files
LS @BIKE_DOCS_STAGE;  -- expect 13 files
LS @SNOW_DOCS_STAGE;  -- expect 7 files
