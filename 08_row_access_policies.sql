-- ============================================================
-- STEP 8: ROW ACCESS POLICIES ON SALES DATA
-- Run as: ACCOUNTADMIN
-- Enforces product-category-level row security on FACT_SALES:
--
--   BIKE_ROLE  → only rows where DIM_ARTICLE.ARTICLE_CATEGORY = 'Bike'
--   SNOW_ROLE  → only rows where DIM_ARTICLE.ARTICLE_CATEGORY IN ('Skis','Ski Boots')
--   ACCOUNTADMIN / SYSADMIN → all rows (unrestricted)
--   Any other role → no rows
--
-- The policy is attached to FACT_SALES.ARTICLE_ID and performs
-- a correlated subquery against DIM_ARTICLE to resolve the
-- product category at query time.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- Row Access Policy definition
-- ============================================================
CREATE OR REPLACE ROW ACCESS POLICY sales_product_rap
AS (article_id NUMBER) RETURNS BOOLEAN ->
    CASE
        -- Admins see everything
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN TRUE

        -- BIKE_ROLE: only rows whose article belongs to the Bike category
        WHEN CURRENT_ROLE() = 'BIKE_ROLE' THEN
            EXISTS (
                SELECT 1
                FROM CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.DIM_ARTICLE da
                WHERE da.ARTICLE_ID       = article_id
                  AND da.ARTICLE_CATEGORY = 'Bike'
            )

        -- SNOW_ROLE: only rows whose article belongs to Skis or Ski Boots
        WHEN CURRENT_ROLE() = 'SNOW_ROLE' THEN
            EXISTS (
                SELECT 1
                FROM CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.DIM_ARTICLE da
                WHERE da.ARTICLE_ID       = article_id
                  AND da.ARTICLE_CATEGORY IN ('Skis', 'Ski Boots')
            )

        -- All other roles: deny access
        ELSE FALSE
    END;

-- ============================================================
-- Attach the policy to FACT_SALES on the ARTICLE_ID column
-- ============================================================
ALTER TABLE FACT_SALES
    ADD ROW ACCESS POLICY sales_product_rap ON (ARTICLE_ID);

-- ============================================================
-- Verification
-- ============================================================

-- Confirm policy exists and is attached
SHOW ROW ACCESS POLICIES IN SCHEMA CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC;

-- As ACCOUNTADMIN: all 6 050 rows visible, all 8 products present
SELECT
    da.ARTICLE_CATEGORY,
    da.ARTICLE_NAME,
    COUNT(*)      AS sale_rows,
    SUM(fs.TOTAL_PRICE) AS total_revenue
FROM FACT_SALES fs
JOIN DIM_ARTICLE da ON fs.ARTICLE_ID = da.ARTICLE_ID
GROUP BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME
ORDER BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME;

-- As BIKE_ROLE: should only see Bike products
USE ROLE BIKE_ROLE;
SELECT
    da.ARTICLE_CATEGORY,
    da.ARTICLE_NAME,
    COUNT(*) AS sale_rows
FROM FACT_SALES fs
JOIN DIM_ARTICLE da ON fs.ARTICLE_ID = da.ARTICLE_ID
GROUP BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME
ORDER BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME;

-- As SNOW_ROLE: should only see Skis / Ski Boots products
USE ROLE SNOW_ROLE;
SELECT
    da.ARTICLE_CATEGORY,
    da.ARTICLE_NAME,
    COUNT(*) AS sale_rows
FROM FACT_SALES fs
JOIN DIM_ARTICLE da ON fs.ARTICLE_ID = da.ARTICLE_ID
GROUP BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME
ORDER BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME;

USE ROLE ACCOUNTADMIN;
