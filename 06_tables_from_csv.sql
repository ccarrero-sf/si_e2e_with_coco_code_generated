-- ============================================================
-- STEP 6: CREATE STRUCTURED TABLES FROM CSV FILES
-- Run as: ACCOUNTADMIN
-- Loads all CSV files from CSV_STAGE into Snowflake tables:
--   DIM_ARTICLE               - Product catalogue (8 articles)
--   DIM_CUSTOMER              - Customer master data
--   FACT_SALES                - Sales transactions
--   CUSTOMER_EXPERIENCE_COMMENTS - Product reviews / feedback
--
-- NOTE: eval_dataset.csv is kept as a stage file only (it is a
--       test/evaluation artefact, not operational data).
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- Shared file format (used for all four COPY INTO commands)
-- ============================================================
CREATE OR REPLACE FILE FORMAT CSV_FMT
    TYPE                        = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                     = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL         = TRUE
    TRIM_SPACE                  = TRUE;

-- ============================================================
-- DIM_ARTICLE  (header row present)
-- ============================================================
CREATE OR REPLACE TABLE DIM_ARTICLE (
    ARTICLE_ID       NUMBER        NOT NULL PRIMARY KEY,
    ARTICLE_NAME     VARCHAR(255)  NOT NULL,
    ARTICLE_CATEGORY VARCHAR(100)  NOT NULL,   -- 'Bike' | 'Skis' | 'Ski Boots'
    ARTICLE_BRAND    VARCHAR(100),
    ARTICLE_COLOR    VARCHAR(100),
    ARTICLE_PRICE    NUMBER(10, 2) NOT NULL
);

COPY INTO DIM_ARTICLE (ARTICLE_ID, ARTICLE_NAME, ARTICLE_CATEGORY,
                        ARTICLE_BRAND, ARTICLE_COLOR, ARTICLE_PRICE)
FROM (
    SELECT $1::NUMBER, $2::VARCHAR, $3::VARCHAR,
           $4::VARCHAR, $5::VARCHAR, $6::NUMBER
    FROM @CSV_STAGE/DIM_ARTICLE.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 1)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- DIM_CUSTOMER  (header row present)
-- ============================================================
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUSTOMER_ID      NUMBER       NOT NULL PRIMARY KEY,
    CUSTOMER_NAME    VARCHAR(255),
    CUSTOMER_REGION  VARCHAR(100),
    CUSTOMER_AGE     NUMBER(3),
    CUSTOMER_GENDER  VARCHAR(50),
    CUSTOMER_SEGMENT VARCHAR(100)   -- e.g. 'Premium' | 'Regular'
);

COPY INTO DIM_CUSTOMER (CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION,
                         CUSTOMER_AGE, CUSTOMER_GENDER, CUSTOMER_SEGMENT)
FROM (
    SELECT $1::NUMBER, $2::VARCHAR, $3::VARCHAR,
           $4::NUMBER, $5::VARCHAR, $6::VARCHAR
    FROM @CSV_STAGE/DIM_CUSTOMER.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 1)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- FACT_SALES  (no header row; gzip compressed)
-- ============================================================
CREATE OR REPLACE TABLE FACT_SALES (
    SALE_ID        NUMBER        NOT NULL PRIMARY KEY,
    ARTICLE_ID     NUMBER        NOT NULL REFERENCES DIM_ARTICLE(ARTICLE_ID),
    DATE_SALES     DATE          NOT NULL,
    CUSTOMER_ID    NUMBER        NOT NULL REFERENCES DIM_CUSTOMER(CUSTOMER_ID),
    QUANTITY_SOLD  NUMBER        NOT NULL,
    TOTAL_PRICE    NUMBER(12, 2) NOT NULL,
    SALES_CHANNEL  VARCHAR(100),             -- 'Online' | 'Partner'
    IS_RETURN      BOOLEAN       DEFAULT FALSE
);

COPY INTO FACT_SALES (SALE_ID, ARTICLE_ID, DATE_SALES, CUSTOMER_ID,
                       QUANTITY_SOLD, TOTAL_PRICE, SALES_CHANNEL, IS_RETURN)
FROM (
    SELECT $1::NUMBER, $2::NUMBER, $3::DATE,
           $4::NUMBER, $5::NUMBER, $6::NUMBER(12,2),
           $7::VARCHAR, $8::BOOLEAN
    FROM @CSV_STAGE/fact_sales.csv_0_0_0.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 0)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- CUSTOMER_EXPERIENCE_COMMENTS  (no header row; gzip compressed)
-- ============================================================
CREATE OR REPLACE TABLE CUSTOMER_EXPERIENCE_COMMENTS (
    COMMENT_ID    NUMBER        NOT NULL PRIMARY KEY,
    COMMENT_DATE  DATE,
    ARTICLE_ID    NUMBER        REFERENCES DIM_ARTICLE(ARTICLE_ID),
    ARTICLE_NAME  VARCHAR(255),
    COMMENT_TEXT  VARCHAR(4000) NOT NULL
);

COPY INTO CUSTOMER_EXPERIENCE_COMMENTS
         (COMMENT_ID, COMMENT_DATE, ARTICLE_ID, ARTICLE_NAME, COMMENT_TEXT)
FROM (
    SELECT $1::NUMBER, $2::DATE, $3::NUMBER,
           $4::VARCHAR, $5::VARCHAR
    FROM @CSV_STAGE/customer_experience_comments.csv_0_0_0.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 0)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- Role grants  (SELECT on all four tables to both roles)
-- ============================================================
GRANT SELECT ON TABLE DIM_ARTICLE                   TO ROLE BIKE_ROLE;
GRANT SELECT ON TABLE DIM_ARTICLE                   TO ROLE SNOW_ROLE;
GRANT SELECT ON TABLE DIM_CUSTOMER                  TO ROLE BIKE_ROLE;
GRANT SELECT ON TABLE DIM_CUSTOMER                  TO ROLE SNOW_ROLE;
GRANT SELECT ON TABLE FACT_SALES                    TO ROLE BIKE_ROLE;
GRANT SELECT ON TABLE FACT_SALES                    TO ROLE SNOW_ROLE;
GRANT SELECT ON TABLE CUSTOMER_EXPERIENCE_COMMENTS  TO ROLE BIKE_ROLE;
GRANT SELECT ON TABLE CUSTOMER_EXPERIENCE_COMMENTS  TO ROLE SNOW_ROLE;

-- ============================================================
-- Verification
-- ============================================================
SELECT 'DIM_ARTICLE'                  AS table_name, COUNT(*) AS row_count FROM DIM_ARTICLE
UNION ALL
SELECT 'DIM_CUSTOMER',                               COUNT(*) FROM DIM_CUSTOMER
UNION ALL
SELECT 'FACT_SALES',                                 COUNT(*) FROM FACT_SALES
UNION ALL
SELECT 'CUSTOMER_EXPERIENCE_COMMENTS',               COUNT(*) FROM CUSTOMER_EXPERIENCE_COMMENTS
ORDER BY 1;
