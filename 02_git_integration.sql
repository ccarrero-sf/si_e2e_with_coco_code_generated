-- ============================================================
-- STEP 2: GIT INTEGRATION
-- Run as: ACCOUNTADMIN
-- Creates an account-level API integration for GitHub and a
-- database-level Git repository pointing to the source files.
-- The API integration persists across database re-creates.
-- Repo is public - no authentication secrets needed.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ---- Account-level API integration for GitHub ----
CREATE OR REPLACE API INTEGRATION GITHUB_SI_E2E_INTEGRATION
    API_PROVIDER         = GIT_HTTPS_API
    API_ALLOWED_PREFIXES = ('https://github.com/ccarrero-sf/')
    ENABLED              = TRUE
    COMMENT              = 'API integration for ccarrero-sf GitHub organization (public repos)';

-- ---- Git repository (database-level object) ----
CREATE OR REPLACE GIT REPOSITORY SI_E2E_FILES_REPO
    API_INTEGRATION = GITHUB_SI_E2E_INTEGRATION
    ORIGIN          = 'https://github.com/ccarrero-sf/si_e2e_with_coco_files'
    COMMENT         = 'Source files for Snowflake Intelligence E2E lab';

-- Fetch latest content from remote
ALTER GIT REPOSITORY SI_E2E_FILES_REPO FETCH;

-- ---- Verification: list files in both folders ----
LS @SI_E2E_FILES_REPO/branches/main/csv/;
LS @SI_E2E_FILES_REPO/branches/main/docs/;
