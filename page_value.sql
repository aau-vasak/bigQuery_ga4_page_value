/* Original query:      https://gist.github.com/nicobrx/49bd948aa093e776ee89df563c790c49
   Google Reference:    https://support.google.com/analytics/answer/2695658 
*/

WITH
  dates AS (
  SELECT
    '20240301' AS start_date,
    FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)) AS end_date),


-- Pulling transaction timestamp and revenue for converted sessions 
-- ga_session_id is not unique to each user and needs to be combined with the user_pseudo_id to ensure it is truly unique
-- session_id will be used later to join page data
  event_values AS (
  SELECT
    CONCAT(user_pseudo_id,' - ',(SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id')) AS session_id,
    event_timestamp,
    SUM(event_value_in_usd) AS event_value
  FROM
    `project_id.analytics_table.events_*`
  WHERE 
    _TABLE_SUFFIX BETWEEN (SELECT start_date from dates) 
    AND (SELECT end_date FROM dates) 
    AND 
    event_value_in_usd is not null
  GROUP BY
    session_id,
    event_params,
    event_timestamp),


-- Retrieving all pages and timestamp of each pageview
  pages AS (
  SELECT
    event_date,
    CONCAT(user_pseudo_id,' - ',(SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id')) AS session_id,
    event_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_title') AS page_title,     
  FROM
    `project_id.analytics_table.events_*`
  WHERE 
    _TABLE_SUFFIX BETWEEN (SELECT start_date from dates) 
    AND (SELECT end_date FROM dates) 
    and 
    event_name = 'page_view'
  ),


-- Joining event value and page CTEs to determine revenue attribution
-- Revenue attribution applies to pages visited before the transaction event 
-- session_id is a unique view, as the calculation focuses on the user viewing the page once rather than counting all pageviews
-- To support dynamic data visualization, the final query should remain unaggregated, providing users the flexibility to aggregate by page location, page title, and select date ranges
  page_event_value AS (
  SELECT
    CAST(FORMAT_DATE('%Y-%m-%d',PARSE_DATE('%Y%m%d',event_date)) AS date) AS date,
    p.session_id AS unique_views,
    page_location,
    page_title,
    CASE WHEN p.event_timestamp > e.event_timestamp THEN 0 ELSE event_value END AS page_revenue
  FROM 
    pages AS p  
  FULL OUTER JOIN
    event_values AS e 
  ON p.session_id = e.session_id)


-- If dynamic visualiztion is not needed, use final query below
-- You can use either page_location or page_title for grouping purposes
SELECT
  page_title,
  SUM(page_revenue)/COUNT(DISTINCT unique_views) AS page_value
FROM
  page_event_value
GROUP BY
  page_title
