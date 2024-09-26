WITH
  dates AS (
  SELECT
    '20240301' AS start_date,
    FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)) AS end_date),


  -- Pulling transaction timestamp and revenue for converted sessions   
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


-- Retrieving all pageviews and page from sessions
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
  )


-- Joining event value and page CTEs to determine revenue attribution
-- Revenue attribution applies to pages visited before the transaction event 
-- Left unaggregated to support dynamic data visualization, allowing users to choose to view page value by either page location or page title and date range
  SELECT
    CAST(FORMAT_DATE('%Y-%m-%d',PARSE_DATE('%Y%m%d',event_date)) AS date) AS date,
    p.session_id,
    page_location,
    page_title,
    CASE WHEN p.event_timestamp > e.event_timestamp THEN 0 ELSE event_value END AS page_revenue
  FROM 
    pages AS p  
  FULL OUTER JOIN
    event_values AS e 
  ON p.session_id = e.session_id
