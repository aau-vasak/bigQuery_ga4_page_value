/* Original query:      https://gist.github.com/nicobrx/49bd948aa093e776ee89df563c790c49
   Google Reference:    https://support.google.com/analytics/answer/2695658 
*/



WITH 
dates as (
  SELECT 
    '20240301' as start_date,
    -- The next line gets yesterday
    FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)) AS end_date),


-- Pulling transaction timestamp and revenue for converted sessions
-- Transaction_id isn't always necessary, but it helps identify pages viewed before specific transactions in sessions with multiple transactions and aids in query QA.
  transactions AS (
  SELECT
    CONCAT(user_pseudo_id,' - ',(SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id')) AS session_id,
    event_timestamp AS revenue_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'transaction_id') AS transaction_id,
    SUM(event_value_in_usd) AS event_value
  FROM
    `project_id.table_id.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates) 
    AND event_value_in_usd IS NOT NULL
    AND event_name = 'purchase'
  GROUP BY
    session_id,
    event_params,
    event_timestamp),


-- Retrieving all pageviews and pages from sessions
pageviews AS (
  SELECT 
    event_date,
    CONCAT(user_pseudo_id,' - ',(SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id')) AS session_id,
    event_timestamp AS page_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_title') AS page_title,     
  FROM
    `project_id.table_id.events_*`
  WHERE 
    _TABLE_SUFFIX BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates) 
    AND event_name = 'page_view'),


-- Filtering for only unique views
-- Unique views refer to pages that were viewed at least once during a session
  unique_views AS (
  SELECT
    DISTINCT event_date,
    session_id,
    page_location,
    page_title
  FROM
    pageviews ),


-- Joining trasactions and pageviews CTEs
-- Assigning revenue to all pages (this is not actual attribution)
-- Adding is_before_revenue_event column to identify pages views prior to transaction event
  page_event_value AS (
  SELECT
    event_date,
    p.session_id,
    page_timestamp,
    page_title,
    transaction_id,
    event_value,
    (CASE WHEN (page_timestamp < revenue_timestamp) THEN TRUE ELSE FALSE END) AS is_before_revenue_event
  FROM
    pageviews AS p
  FULL OUTER JOIN
    transactions AS t
  ON
    p.session_id = t.session_id),


-- Filtering for pages prior to transaction event to make sure revenue attribution are only to those
-- Using DISTINCT and removing page_timestamp to dedupe
  pages_before_revenue_event AS (
  SELECT
    DISTINCT * EXCEPT(page_timestamp)
  FROM
    page_event_value
  WHERE
    is_before_revenue_event IS TRUE)


-- Final query
-- Aggregating page revenue by page, session and date
-- Each session_id represents a unique view for the corresponding page
-- This format will be maintained for a dynamic dashboard view
-- If a dynamic view is not needed proceed with calculating page value by page: SUM(page_revenue)/COUNT(DISTINCT session_id)
SELECT
  CAST(FORMAT_DATE('%Y-%m-%d',PARSE_DATE('%Y%m%d',a.event_date)) AS date) AS date,
  a.session_id,
  a.page_location,
  a.page_title,
  SUM(event_value) AS page_revenue
FROM
  unique_views AS a
LEFT JOIN
  pages_before_revenue_event AS b
ON
  a.event_date = b.event_date
  AND a.session_id = b.session_id
  AND a.page_title = b.page_title
GROUP BY
  1,
  2,
  3,
  4
