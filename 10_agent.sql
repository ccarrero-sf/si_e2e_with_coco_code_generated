-- ============================================================
-- STEP 10: SALES EXPERT AGENT
-- Run as: ACCOUNTADMIN
-- ============================================================
-- Creates the SALES_EXPERT_AGENT for Snowflake Intelligence with
-- four tools:
--   1. SalesDataAnalytics         - Cortex Analyst (SALES_DATA_SEMANTIC_VIEW)
--   2. ProductDocumentationSearch - Cortex Search  (DOCUMENTATION_TOOL)
--   3. CustomerFeedbackSearch     - Cortex Search  (CUSTOMER_FEEDBACK_TOOL)
--   4. SendEmailSummary           - Generic / SP   (SEND_SUMMARY_EMAIL)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- PART A: EMAIL INFRASTRUCTURE
-- ============================================================

-- Email notification integration required by SYSTEM$SEND_EMAIL
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS SI_E2E_EMAIL_INTEGRATION
    TYPE    = EMAIL
    ENABLED = TRUE;

GRANT USAGE ON INTEGRATION SI_E2E_EMAIL_INTEGRATION TO ROLE BIKE_ROLE;
GRANT USAGE ON INTEGRATION SI_E2E_EMAIL_INTEGRATION TO ROLE SNOW_ROLE;

-- Stored procedure the agent calls to send email summaries.
-- Returns a confirmation string (or graceful error) so the
-- agent can report success/failure back to the user.
CREATE OR REPLACE PROCEDURE SEND_SUMMARY_EMAIL(
    RECIPIENT_EMAIL  VARCHAR,
    SUBJECT          VARCHAR,
    BODY_TEXT        VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CALL SYSTEM$SEND_EMAIL(
        'SI_E2E_EMAIL_INTEGRATION',
        :RECIPIENT_EMAIL,
        :SUBJECT,
        :BODY_TEXT
    );
    RETURN 'Email summary sent successfully to ' || :RECIPIENT_EMAIL;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Note: Email delivery unavailable in this environment. ' ||
               'Key findings: ' || :BODY_TEXT;
END;
$$;

GRANT USAGE ON PROCEDURE SEND_SUMMARY_EMAIL(VARCHAR, VARCHAR, VARCHAR) TO ROLE BIKE_ROLE;
GRANT USAGE ON PROCEDURE SEND_SUMMARY_EMAIL(VARCHAR, VARCHAR, VARCHAR) TO ROLE SNOW_ROLE;

-- ============================================================
-- PART B: CREATE SALES_EXPERT_AGENT
-- ============================================================

CREATE OR REPLACE AGENT CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.SALES_EXPERT_AGENT
FROM SPECIFICATION $$
{
  "models": {
    "orchestration": "auto"
  },
  "orchestration": {
    "budget": {
      "seconds": 900,
      "tokens": 400000
    }
  },
  "instructions": {
    "orchestration": "You are the Sales Expert Agent for a bike and snow sports retailer. You help users understand product performance, customer satisfaction, and sales trends across two product lines: Bikes and Snow Sports.\n\nPRODUCT PORTFOLIO:\n- Bikes: Mondracer Infant Bike ($3,000, brand: Mondracer), Premium Bicycle ($9,000, brand: Veloci), The Ultimate Downhill Bike ($10,000, brand: Graviton), The Xtreme Road Bike 105 SL ($8,500, brand: Xtreme)\n- Snow Sports: Carver Skis ($790, brand: Carver), Outpiste Skis ($900, brand: Outpiste), Racing Fast Skis ($950, brand: RacerX), Ski Boots TDBootz Special ($600, brand: TDBootz)\n\nTOOL SELECTION RULES:\n\n1. SalesDataAnalytics - Use for ALL quantitative sales questions:\n   - Revenue totals, revenue by product, revenue by period, year-over-year comparisons\n   - Units sold, transaction counts, average order value\n   - Customer demographics: region (North/South/East/West), age group, gender, segment (Premium/Regular)\n   - Channel breakdown: Online vs Partner\n   - Product rankings: top or bottom by revenue or quantity\n   - Return analysis: filtering by IS_RETURN = TRUE\n   - Time-series: monthly trends, seasonal patterns, date range filtering on DATE_SALES\n\n2. ProductDocumentationSearch - Use for technical and specification questions:\n   - Product features, components, materials, dimensions\n   - Maintenance procedures and lubrication recommendations\n   - Assembly instructions and setup guides\n   - Safety guidelines and rider recommendations\n   - Warranty terms and coverage\n\n3. CustomerFeedbackSearch - Use for voice-of-customer questions:\n   - Product reviews, satisfaction scores, reported defects\n   - Quality issues (e.g., frame cracks, binding failures)\n   - Warranty claim experiences and dispute history\n   - Recurring complaint or praise themes by product\n\n4. SendEmailSummary - Use ONLY when the user explicitly asks to send an email:\n   - Ask for the recipient email address if not already provided\n   - Compose a concise professional summary (intro, bullet findings, closing)\n   - Keep the body under 500 words\n\nMULTI-TOOL WORKFLOWS:\n- Product comparison: use SalesDataAnalytics for sales performance AND CustomerFeedbackSearch for sentiment, then synthesize both into a unified answer.\n- Quality investigation: use CustomerFeedbackSearch to identify issues, ProductDocumentationSearch for specification context, SalesDataAnalytics to check if quality issues are impacting sales.\n- Product recommendation: combine documentation specs, customer feedback scores, and sales data together.\n\nEMAIL OFFER RULE: After answering any complex multi-part question, or whenever you used 2 or more tools to answer a single request, always end your response by asking: Would you like me to send a summary of this analysis to your email?\n\nROLE-BASED ACCESS NOTE: Sales data is automatically filtered at the database level. BIKE_ROLE users see only bike product sales. SNOW_ROLE users see only snow and ski product sales. ACCOUNTADMIN sees all data. Do not try to work around this filtering.",
    "response": "TONE: Professional, knowledgeable, and concise. You are a subject-matter expert advising business users and sales professionals.\n\nFORMAT RULES:\n- Lead with the direct answer, then provide supporting data and context.\n- Use markdown tables for comparisons involving 3 or more products or metrics.\n- Use bullet points for lists of features, quality issues, or findings.\n- Include units with all numbers: currency as $X,XXX format, quantities as X units, percentages as X%.\n- Bold key metrics and product names for scannability.\n\nANALYSIS STRUCTURE (for analytical questions):\n1. Direct Answer\n2. Supporting Data (table or list)\n3. Key Insight\n4. Recommendation (when applicable)\n\nEMAIL CLOSE: After any substantial multi-tool analysis, close with:\nWould you like me to send a summary of this analysis to your email?"
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "SalesDataAnalytics",
        "description": "Queries structured sales data to answer quantitative business questions about revenue, product performance, customer segments, and sales trends.\n\nData Coverage: 6,050 sales transactions from 2022 through 2025. Covers all 8 products (4 bikes, 3 ski models, 1 ski boots model) across 5,000 customers.\n\nWhen to Use:\n- Revenue: total sales, revenue by product/period/region/channel, year-over-year comparisons\n- Volume: units sold, transaction counts, quantity per product\n- Customers: breakdown by region (North/South/East/West), age group, gender, segment (Premium/Regular)\n- Channel: Online vs Partner performance\n- Rankings: top or bottom products by revenue or quantity\n- Returns: return rates, IS_RETURN filtering\n- Trends: monthly or seasonal patterns, date range analysis\n\nKey Metrics: total_revenue, transaction_count, units_sold, unique_customers, avg_order_value\n\nWhen NOT to Use:\n- Product technical specifications or features → use ProductDocumentationSearch\n- Customer reviews or complaints → use CustomerFeedbackSearch\n- Questions that cannot be answered from numerical sales data"
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "ProductDocumentationSearch",
        "description": "Searches product documentation including PDF user guides and product image descriptions to answer technical and specification questions about all 8 products.\n\nData Coverage: PDF guides and product images for all products. Bikes: Mondracer Infant Bike, Premium Bicycle, The Xtreme Road Bike 105 SL, The Ultimate Downhill Bike. Snow: Carver Skis, OutPiste Skis, RacingFast Skis, Ski Boots TDBootz Special.\n\nWhen to Use:\n- Product features and specifications (frame materials, components, dimensions, weight)\n- Maintenance instructions and lubrication recommendations\n- Assembly and setup guides\n- Safety guidelines and rider level recommendations\n- Warranty terms and coverage details\n- Technical performance characteristics\n- Specification-based product comparisons\n\nWhen NOT to Use:\n- Sales figures or revenue data → use SalesDataAnalytics\n- Customer opinions or complaints → use CustomerFeedbackSearch\n- Questions requiring numerical aggregation"
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "CustomerFeedbackSearch",
        "description": "Searches 52 customer reviews and feedback comments to answer questions about product satisfaction, quality issues, defects, and customer experience.\n\nData Coverage: 52 reviews covering all 8 products. Reviews include satisfaction scores, specific complaints (e.g., frame defects, binding issues, warranty disputes), and positive endorsements. Date range: 2023-2025.\n\nWhen to Use:\n- Customer satisfaction and sentiment (overall or per product)\n- Product quality issues and defects (e.g., frame cracks, premature wear)\n- Warranty claim experiences and dispute outcomes\n- Positive endorsements and praise themes\n- Comparing customer perception across products\n- Understanding recurring complaint or satisfaction patterns\n\nWhen NOT to Use:\n- Sales figures or revenue data → use SalesDataAnalytics\n- Technical product specifications → use ProductDocumentationSearch\n- Questions about future products or forecast"
      }
    },
    {
      "tool_spec": {
        "type": "generic",
        "name": "SendEmailSummary",
        "description": "Sends a plain-text email summary of the analysis or conversation findings to a specified recipient email address.\n\nWhen to Use: ONLY when the user explicitly requests an email. Always ask for the recipient email address if not already provided in the conversation.\n\nWhen NOT to Use: Do not call proactively or without explicit user request. Do not use for operational alerts.\n\nBest Practices:\n- Use a descriptive subject line, e.g.: Sales Analysis Summary - Bike Products Q4 2025\n- Structure body with: brief introduction, key findings as bullet points, closing note\n- Keep body_text under 500 words for readability",
        "input_schema": {
          "type": "object",
          "properties": {
            "recipient_email": {
              "type": "string",
              "description": "Email address of the recipient. Must be a valid format (user@domain.com). Ask the user for this if not already provided."
            },
            "subject": {
              "type": "string",
              "description": "Email subject line clearly describing the content, e.g.: Sales Analysis Summary - Bike Products Q4 2025."
            },
            "body_text": {
              "type": "string",
              "description": "Plain text body. Include a brief introduction, key findings as bullet points, and a closing note. Keep under 500 words."
            }
          },
          "required": ["recipient_email", "subject", "body_text"]
        }
      }
    }
  ],
  "tool_resources": {
    "SalesDataAnalytics": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": "COMPUTE_WH"
      },
      "semantic_view": "CC_COCO_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.SALES_DATA_SEMANTIC_VIEW"
    },
    "ProductDocumentationSearch": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": "COMPUTE_WH"
      },
      "search_service": "CC_COCO_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.DOCUMENTATION_TOOL"
    },
    "CustomerFeedbackSearch": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": "COMPUTE_WH"
      },
      "search_service": "CC_COCO_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.CUSTOMER_FEEDBACK_TOOL"
    },
    "SendEmailSummary": {
      "type": "procedure",
      "identifier": "CC_COCO_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.SEND_SUMMARY_EMAIL",
      "execution_environment": {
        "type": "warehouse",
        "warehouse": "COMPUTE_WH",
        "query_timeout": 60
      }
    }
  }
}
$$;

-- ============================================================
-- PART C: GRANTS
-- ============================================================

GRANT USAGE ON AGENT CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.SALES_EXPERT_AGENT
    TO ROLE BIKE_ROLE;

GRANT USAGE ON AGENT CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.SALES_EXPERT_AGENT
    TO ROLE SNOW_ROLE;

-- ============================================================
-- PART D: VERIFICATION
-- ============================================================

-- List agents in the schema
SHOW AGENTS IN SCHEMA CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC;

-- Describe the agent (shows tools and spec)
DESCRIBE AGENT CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.SALES_EXPERT_AGENT;
