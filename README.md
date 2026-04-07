# Snowflake Intelligence End-to-End Lab — Setup

This directory contains all SQL scripts needed to provision the Snowflake environment for the **Snowflake Intelligence E2E lab** from scratch. Running the scripts in order produces a fully reproducible environment.

---

## Repository structure

```
si_e2e_with_coco_code_generated/
├── 01_setup.sql                    # Database, roles, users, grants
├── 02_git_integration.sql          # GitHub API integration + Git repository
├── 03_stages_and_ingestion.sql     # Internal stages + copy files from Git
├── 04_data_processing.sql          # AI PDF/image pipeline → DOCS_CHUNKS_TABLE
├── 05_cortex_search.sql            # Cortex Search Service (documentation)
├── 06_tables_from_csv.sql          # Structured tables from CSV stage files
├── 07_customer_feedback_search.sql # Cortex Search Service (customer feedback)
├── 08_row_access_policies.sql      # Row-level security on FACT_SALES
├── 09_semantic_view.sql            # Semantic View for Cortex Analyst
├── 10_agent.sql                    # SALES_EXPERT_AGENT + email infrastructure
├── semantic_view_work/
│   └── SALES_DATA_SEMANTIC_VIEW_semantic_model.yaml  # FastGen-generated YAML
└── README.md                       # This file
```

---

## Prerequisites

| Requirement | Value |
|---|---|
| Snowflake role | `ACCOUNTADMIN` |
| Warehouse | `COMPUTE_WH` |
| GitHub repo (public) | `https://github.com/ccarrero-sf/si_e2e_with_coco_files` |

---

## Execution order

Run scripts in numbered order. Each script is idempotent and can be re-run safely. **The database is dropped and recreated on every run of `01_setup.sql`.**

### Step 1 — Database, roles, users, grants

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 01_setup.sql
```

**What it does:**
- Drops and recreates `CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E`
- Creates roles `BIKE_ROLE` and `SNOW_ROLE` (if not exist)
- Creates demo users `BIKE_USER` and `SNOW_USER` (if not exist)
- Grants `COMPUTE_WH` usage and database/schema access to both roles
- Grants both roles to `CCARRERO` (admin user)

### Step 2 — Git integration

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 02_git_integration.sql
```

**What it does:**
- Creates account-level API integration `GITHUB_SI_E2E_INTEGRATION` for GitHub (public repo, no auth required)
- Creates database-level Git repository `SI_E2E_FILES_REPO`
- Fetches latest content from `main` branch

### Step 3 — Stages and data ingestion

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 03_stages_and_ingestion.sql
```

**What it does:**
- Creates three internal stages with **Snowflake SSE encryption** and directory tables enabled
- Grants role-based read access to stages
- Copies files from the Git repository into the appropriate stages

### Step 4 — AI data processing

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 04_data_processing.sql
```

**What it does:**
- Enables directory tables on `BIKE_DOCS_STAGE` and `SNOW_DOCS_STAGE`
- Creates `DOCS_CHUNKS_TABLE` to hold all processed content
- **PDF pipeline**: parses each PDF with `AI_PARSE_DOCUMENT` (LAYOUT mode), splits text into chunks of ~1500 chars (100 char overlap) using `SPLIT_TEXT_RECURSIVE_CHARACTER`, classifies each file as `BIKE` or `SNOW` using `AI_CLASSIFY` on filename + first 500 chars
- **Image pipeline**: generates a detailed product description for each image using `AI_COMPLETE` (`claude-3-7-sonnet`), classifies as `BIKE` or `SNOW` using `AI_CLASSIFY` on the filename
- Runs post-insert sanity checks: row counts, NULL checks, per-file chunk counts, classification accuracy

### Step 5 — Cortex Search Service

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 05_cortex_search.sql
```

**What it does:**
- Creates `DOCUMENTATION_TOOL` Cortex Search Service over `DOCS_CHUNKS_TABLE`
- Embedding model: `snowflake-arctic-embed-l-v2.0`
- Refresh lag: `1 day`
- Grants service usage to `BIKE_ROLE` and `SNOW_ROLE`
- Previews results for sample bike and snow queries

### Step 6 — Structured tables from CSV files

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 06_tables_from_csv.sql
```

**What it does:**
- Creates a shared `CSV_FMT` file format (CSV, double-quote enclosure, null-aware)
- Creates and loads four tables from `@CSV_STAGE`:

| Table | Source file | Rows | Header |
|---|---|---|---|
| `DIM_ARTICLE` | `DIM_ARTICLE.csv` | 8 | Yes |
| `DIM_CUSTOMER` | `DIM_CUSTOMER.csv` | 5 000 | Yes |
| `FACT_SALES` | `fact_sales.csv_0_0_0.csv.gz` | 6 050 | No |
| `CUSTOMER_EXPERIENCE_COMMENTS` | `customer_experience_comments.csv_0_0_0.csv.gz` | 52 | No |

- Grants `SELECT` on all four tables to both `BIKE_ROLE` and `SNOW_ROLE`

### Step 7 — Customer feedback Cortex Search Service

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 07_customer_feedback_search.sql
```

**What it does:**
- Creates `CUSTOMER_FEEDBACK_TOOL` Cortex Search Service over `CUSTOMER_EXPERIENCE_COMMENTS.COMMENT_TEXT`
- Embedding model: `snowflake-arctic-embed-l-v2.0` | Refresh lag: `1 day`
- Filter attributes: `COMMENT_ID`, `COMMENT_DATE`, `ARTICLE_ID`, `ARTICLE_NAME`
- Grants service usage to `BIKE_ROLE` and `SNOW_ROLE`
- The agent uses this service to answer questions about product reviews and customer sentiment

### Step 8 — Row Access Policies on sales data

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 08_row_access_policies.sql
```

**What it does:**
- Creates row access policy `sales_product_rap` with the following logic:

| Role | Visible rows |
|---|---|
| `ACCOUNTADMIN` / `SYSADMIN` | All rows |
| `BIKE_ROLE` | Only rows where `DIM_ARTICLE.ARTICLE_CATEGORY = 'Bike'` |
| `SNOW_ROLE` | Only rows where `DIM_ARTICLE.ARTICLE_CATEGORY IN ('Skis', 'Ski Boots')` |
| Any other role | No rows |

- Attaches the policy to `FACT_SALES` on the `ARTICLE_ID` column
- The policy performs a correlated subquery into `DIM_ARTICLE` to resolve the product category at query time — no denormalisation needed

### Step 9 — Semantic View for Cortex Analyst

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 09_semantic_view.sql
```

**What it does:**
- Generates the semantic model YAML using `SYSTEM$CORTEX_ANALYST_FAST_GENERATION` (FastGen)
- Validates the YAML with `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(..., TRUE)` (verify-only) before deploying
- Deploys `SALES_DATA_SEMANTIC_VIEW` to `CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC`
- Grants `SELECT` on the semantic view to `BIKE_ROLE` and `SNOW_ROLE`

**Semantic view summary:**

| Element | Details |
|---|---|
| Name | `SALES_DATA_SEMANTIC_VIEW` |
| Tables | `FACT_SALES`, `DIM_ARTICLE`, `DIM_CUSTOMER` |
| Relationships | `FACT_SALES → DIM_ARTICLE` (many-to-one, `ARTICLE_ID`) |
| | `FACT_SALES → DIM_CUSTOMER` (many-to-one, `CUSTOMER_ID`) |
| Metrics | `total_revenue`, `transaction_count`, `units_sold`, `unique_customers`, `avg_order_value` |
| VQRs | 4 verified queries seeded from `eval_dataset.csv` |

The YAML source file is saved in `semantic_view_work/SALES_DATA_SEMANTIC_VIEW_semantic_model.yaml`.

**Sample questions the semantic view supports (from `eval_dataset.csv`):**
- *What is the total revenue for the Carver Skis during last year?*
- *What ski products have the highest and lowest sales?*
- *What is the total revenue by customer region and product category?*
- *What are the top performing products by total revenue excluding returns?*

### Step 10 — Snowflake Intelligence Agent

```sql
-- Run in Snowsight or SnowSQL:
-- Contents of 10_agent.sql
```

**What it does:**

**Part A — Email infrastructure:**
- Creates `SI_E2E_EMAIL_INTEGRATION` notification integration (`TYPE = EMAIL, ENABLED = TRUE`) required by `SYSTEM$SEND_EMAIL`
- Creates stored procedure `SEND_SUMMARY_EMAIL(RECIPIENT_EMAIL, SUBJECT, BODY_TEXT)` that wraps `SYSTEM$SEND_EMAIL`; includes an exception handler so the agent returns a graceful message if email delivery is unavailable
- Grants `USAGE` on the integration and procedure to `BIKE_ROLE` and `SNOW_ROLE`

**Part B — Agent creation:**
- Creates `SALES_EXPERT_AGENT` in `CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC` using `CREATE OR REPLACE AGENT ... FROM SPECIFICATION $$...$$`
- Grants `USAGE` on the agent to `BIKE_ROLE` and `SNOW_ROLE`

**Part C — Verification:**
- `SHOW AGENTS` confirms the agent exists and is owned by `ACCOUNTADMIN`
- `DESCRIBE AGENT` confirms the full JSON spec (tools + tool_resources + instructions) was stored correctly

**Agent architecture:**

| Tool name | Tool type | Backed by | Purpose |
|---|---|---|---|
| `SalesDataAnalytics` | `cortex_analyst_text_to_sql` | `SALES_DATA_SEMANTIC_VIEW` | Revenue, units sold, rankings, trends, customer segments, channel analysis |
| `ProductDocumentationSearch` | `cortex_search` | `DOCUMENTATION_TOOL` | Product specs, features, maintenance guides, warranty terms |
| `CustomerFeedbackSearch` | `cortex_search` | `CUSTOMER_FEEDBACK_TOOL` | Customer reviews, quality complaints, satisfaction scores |
| `SendEmailSummary` | `generic` (stored procedure) | `SEND_SUMMARY_EMAIL` | Send plain-text email summary of analysis findings |

**Agent behaviour highlights:**
- **Role-aware**: orchestration instructions inform the agent that sales data is filtered at the database level (`BIKE_ROLE` → bikes only, `SNOW_ROLE` → snow/ski only)
- **Multi-tool synthesis**: agent is instructed to combine `SalesDataAnalytics` + `CustomerFeedbackSearch` for comparative product questions, and all three data tools for quality-impact investigations
- **Email offer rule**: after any response that used 2+ tools, the agent always closes by asking if the user wants a summary sent to their email
- **Email workflow**: agent asks for the recipient address if not provided, composes a structured summary (intro + bullet findings + closing), then calls `SendEmailSummary`

**Sample questions the agent handles:**
- *What are the top-selling bike products by revenue this year?*
- *What technical specs distinguish the Xtreme Road Bike from the Ultimate Downhill Bike?*
- *Are there recurring quality complaints about the Carver Skis, and is it affecting sales?*
- *Compare customer satisfaction and revenue performance for all ski products.*
- *Send me a summary of this analysis by email.*

---

## Architecture

### Roles and access model

| Role | User | Warehouse | Database | CSV Stage | Bike Docs | Snow Docs | FACT_SALES rows |
|---|---|---|---|---|---|---|---|
| `BIKE_ROLE` | `BIKE_USER` | `COMPUTE_WH` | R/W | Read | Read | — | Bike products only |
| `SNOW_ROLE` | `SNOW_USER` | `COMPUTE_WH` | R/W | Read | — | Read | Skis + Ski Boots only |

### Structured tables

| Table | Source | Rows | Key columns |
|---|---|---|---|
| `DIM_ARTICLE` | `DIM_ARTICLE.csv` | 8 | `ARTICLE_ID`, `ARTICLE_NAME`, `ARTICLE_CATEGORY`, `ARTICLE_BRAND`, `ARTICLE_COLOR`, `ARTICLE_PRICE` |
| `DIM_CUSTOMER` | `DIM_CUSTOMER.csv` | 5 000 | `CUSTOMER_ID`, `CUSTOMER_NAME`, `CUSTOMER_REGION`, `CUSTOMER_AGE`, `CUSTOMER_GENDER`, `CUSTOMER_SEGMENT` |
| `FACT_SALES` | `fact_sales.csv_0_0_0.csv.gz` | 6 050 | `SALE_ID`, `ARTICLE_ID`, `DATE_SALES`, `CUSTOMER_ID`, `QUANTITY_SOLD`, `TOTAL_PRICE`, `SALES_CHANNEL`, `IS_RETURN` |
| `CUSTOMER_EXPERIENCE_COMMENTS` | `customer_experience_comments.csv_0_0_0.csv.gz` | 52 | `COMMENT_ID`, `COMMENT_DATE`, `ARTICLE_ID`, `ARTICLE_NAME`, `COMMENT_TEXT` |

### Cortex Search Services

| Service | Source table | Search column | Purpose |
|---|---|---|---|
| `DOCUMENTATION_TOOL` | `DOCS_CHUNKS_TABLE` | `CHUNK_TEXT` | PDF + image product documentation |
| `CUSTOMER_FEEDBACK_TOOL` | `CUSTOMER_EXPERIENCE_COMMENTS` | `COMMENT_TEXT` | Customer reviews and product feedback |

### Stages

| Stage | Encryption | Contents | Accessible by |
|---|---|---|---|
| `CSV_STAGE` | Snowflake SSE | `DIM_ARTICLE.csv`, `DIM_CUSTOMER.csv`, `eval_dataset.csv`, `customer_experience_comments.csv_0_0_0.csv.gz`, `fact_sales.csv_0_0_0.csv.gz` | Both roles |
| `BIKE_DOCS_STAGE` | Snowflake SSE | Bike PDFs (3) + Bike images (10) | `BIKE_ROLE` only |
| `SNOW_DOCS_STAGE` | Snowflake SSE | Ski PDFs (4) + Ski images (3) | `SNOW_ROLE` only |

### Bike documents (`BIKE_DOCS_STAGE`)

- `Mondracer Infant Bike.pdf`
- `Premium_Bicycle_User_Guide.pdf`
- `The Xtreme Road Bike 105 SL.pdf`
- `The_Ultimate_Downhill_Bike.pdf`
- `Premium_Bicycle_1.jpeg` through `Premium_Bicycle_4.jpeg`
- `The_Ultimate_Downhill_Bike_1.jpeg`, `The_Ultimate_Downhill_Bike_2.jpeg`
- `The_Xtreme_Road_Bike_3.jpeg` through `The_Xtreme_Road_Bike_5.jpeg`

### Snow/Ski documents (`SNOW_DOCS_STAGE`)

- `Carver Skis Specification Guide.pdf`
- `OutPiste Skis Specification Guide.pdf`
- `RacingFast Skis Specification Guide.pdf`
- `Ski_Boots_TDBootz_Special.pdf`
- `Outpiste_Skis.jpeg`
- `Racing_Fast_Skis.jpeg`
- `Ski_Boots_TDBootz_Special.jpg`

---

## Demo user credentials

> These credentials are for lab/demo use only.

| User | Password | Default Role |
|---|---|---|
| `BIKE_USER` | `xxxxx!` | `BIKE_ROLE` |
| `SNOW_USER` | `xxxxx!` | `SNOW_ROLE` |

---

## Re-running from scratch

Simply execute all ten scripts in numbered order. The database is fully dropped and recreated by `01_setup.sql`. The API integration (account-level) is replaced in place by `02_git_integration.sql`. Scripts `06`–`10` use `CREATE OR REPLACE` so they are also fully idempotent.

> **Note on `SI_E2E_EMAIL_INTEGRATION`**: this is an account-level object created with `IF NOT EXISTS`. It will not be recreated if it already exists. Drop it manually before re-running `10_agent.sql` if you need a clean reset.
