-- ============================================================
-- STEP 1: SETUP - Database, Roles, Users, Grants
-- Run as: ACCOUNTADMIN
-- NOTE: Database is DROPPED and RECREATED on every run.
--       Roles and users are created only if they do not exist.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- Drop and recreate the database fresh on every run
CREATE OR REPLACE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E
    COMMENT = 'Snowflake Intelligence End-to-End Lab database';

USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ---- Roles ----
-- Using IF NOT EXISTS so any account-level grants to these roles are preserved
CREATE ROLE IF NOT EXISTS BIKE_ROLE
    COMMENT = 'Role for bicycle product users';

CREATE ROLE IF NOT EXISTS SNOW_ROLE
    COMMENT = 'Role for ski/snow product users';

-- ---- Demo Users ----
CREATE USER IF NOT EXISTS BIKE_USER
    PASSWORD            = 'BikeDemoPass123!'
    DEFAULT_ROLE        = BIKE_ROLE
    DEFAULT_WAREHOUSE   = COMPUTE_WH
    DEFAULT_NAMESPACE   = 'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC'
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT             = 'Demo user for bike product access';

CREATE USER IF NOT EXISTS SNOW_USER
    PASSWORD            = 'SnowDemoPass123!'
    DEFAULT_ROLE        = SNOW_ROLE
    DEFAULT_WAREHOUSE   = COMPUTE_WH
    DEFAULT_NAMESPACE   = 'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC'
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT             = 'Demo user for ski/snow product access';

-- ---- Warehouse grants ----
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE BIKE_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SNOW_ROLE;

-- ---- Database grants ----
GRANT USAGE ON DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E TO ROLE BIKE_ROLE;
GRANT USAGE ON DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E TO ROLE SNOW_ROLE;

-- ---- Schema grants ----
GRANT USAGE ON SCHEMA CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC TO ROLE BIKE_ROLE;
GRANT USAGE ON SCHEMA CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC TO ROLE SNOW_ROLE;

-- ---- Assign roles to demo users ----
GRANT ROLE BIKE_ROLE TO USER BIKE_USER;
GRANT ROLE SNOW_ROLE TO USER SNOW_USER;

-- ---- Grant roles to the current admin user ----
GRANT ROLE BIKE_ROLE TO USER CCARRERO;
GRANT ROLE SNOW_ROLE TO USER CCARRERO;

-- ---- Verification ----
SHOW ROLES LIKE '%_ROLE';
SHOW USERS LIKE '%_USER';
SHOW GRANTS TO ROLE BIKE_ROLE;
SHOW GRANTS TO ROLE SNOW_ROLE;
