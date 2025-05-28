CREATE DATABASE IF NOT EXISTS CUSTOMER_LTV_DATA;

USE DATABASE CUSTOMER_LTV_DATA;
USE SCHEMA PUBLIC;
CREATE OR REPLACE STAGE SEMANTIC directory = (enable=true);

CREATE OR REPLACE TABLE PROMPTS (
    user_query STRING,
    llm_prompt STRING
);



INSERT INTO PROMPTS (user_query, llm_prompt) VALUES
('What is the average time spent on our website for male customers who have a lifetime value above $1000?', 
'Please provide a natural language summary of the SQL results. Make it conversational and easy to understand for business stakeholders.

Example:
SQL Result: AVG_WEBSITE_TIME = 45.3
Summary: "On average, our high-value male customers spend about 45 minutes on our website per session."

For the current SQL result, please provide a similar summary:'
),


('Show me the distribution of customer status levels across different gender categories', 
'Transform these SQL results into a clear business insight. Include percentages and highlight any notable patterns.

Example:
SQL Result: 
GENDER | STATUS | COUNT | PERCENTAGE
F      | Gold   | 500   | 25%
F      | Silver | 1000  | 50%
Summary: "Among female customers, half hold Silver status, while a quarter are Gold members."

Please provide a similar comprehensive summary for the current results:'), 


('What"s the correlation between time spent on our app and customer lifetime value?',


'Please explain the correlation results in simple terms that a non-technical audience would understand.

Example:
SQL Result: CORRELATION_COEFFICIENT = 0.75
Summary: "Theres a strong positive relationship between app usage and customer value - as customers spend more time on our app, they tend to generate more revenue."

Please provide a similar interpretation for these results:'),

('Which customer status level has shown the highest average lifetime value in the past year?',

'Convert these SQL findings into actionable business insights. Include specific numbers but present them in a digestible format.

Example:
SQL Result: 
STATUS | AVG_LTV
Gold   | $2500
Silver | $1200
Summary: "Gold status customers are our most valuable segment, spending on average more than twice as much ($2,500) compared to Silver members ($1,200)."

Please create a similar insight-driven summary for these results:'),

('Compare the average website and app engagement time between active and churned customers',
'Please translate these SQL results into a business-friendly narrative that highlights key differences and potential implications.

Example:
SQL Result:
STATUS  | AVG_WEB_TIME | AVG_APP_TIME
Active  | 35.5         | 42.3
Churned | 12.4         | 15.7
Summary: "Active customers show significantly higher engagement, spending roughly three times more time on both our website (36 minutes) and app (42 minutes) compared to customers who have churned."

Based on the current SQL results, please provide a similar comparative analysis:');


 -- Create Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE PROMPT_SEARCH_CUSTOMER_LTV
  ON USER_QUERY
  ATTRIBUTES LLM_PROMPT
  WAREHOUSE = SMALL
  TARGET_LAG = '365 days'
  AS (
    SELECT
        USER_QUERY,
        LLM_PROMPT
    FROM PROMPTS
);
