-- Each line in the table above needs to be calculated separately in 
-- the SQL query and joined together using UNION.  
-- Lastly, to prevent integer division when calculating the 
-- participating_users_pct, one of the values in the quotient needs 
-- to be cast to type <i>NUMERIC</i>.

-- Question 1: How many gaming events, registered users, and participating
-- users are there?
WITH row_summary_stats AS (
--Multiple rows in the event_performance table correspond
-- to the same gaming event, and DISTINCT is needed to identify how
--  many unique dates are in the dataset.  Since there is at most one
--  gaming event per day, the number of unique dates will reveal
-- how many gaming events occurred over the year.  
  SELECT 
    1 AS num
    , 'num_gaming_events' AS statistic
    , COUNT(DISTINCT event_date) AS value
  FROM
    event_performance
  UNION

--Calculating the number of days in a 365 day year that had a gaming event
  SELECT 
    2
    , 'gaming_event_yrly_pct'
    , ROUND(
        (SELECT 
           COUNT(DISTINCT event_date) 
         FROM 
           event_performance
          )::NUMERIC / 365, 4
        ) * 100
  UNION
-- Calculating the number of registered users
  SELECT 
    3
    , 'num_unique_users'
    , COUNT(userid)
  FROM
    users
  UNION
-- Calculating the number of participating users, those
-- who played in at least one gaming event.
  SELECT 
    4
    , 'participating_users_pct'
    , ROUND(
        (SELECT 
           COUNT(DISTINCT userid) 
         FROM
           event_performance
          )::NUMERIC / 
        (SELECT 
           COUNT(userid)
         FROM
           users), 4
        ) * 100
    )

SELECT 
  statistic
  , value
FROM
  row_summary_stats
ORDER BY
  num;

--Results:
--         statistic        |  value  
-- -------------------------+---------
--  num_gaming_events       |     147
--  gaming_event_yrly_pct   | 40.2700
--  num_unique_users        |    1100
--  participating_users_pct | 89.0900

-- Question 2: What proportion of users are subscribers?
-- Note:
-- SUM(COUNT ) OVER () will return the sum of the grouped counts
-- thus giving the total count of the entire dataset
SELECT 
  subscriber
  , ROUND(
      COUNT(userid):: numeric / SUM(
        COUNT(userid)
      ) OVER (), 
      3
    ) AS proportion 
FROM 
  users 
GROUP BY 
  subscriber 
ORDER BY 
  subscriber;

--Results:
--  subscriber | proportion 
-- ------------+------------
--           0 |      0.817
--           1 |      0.183

-- Question 3:   What proportion of users come from Canada, the US, and Mexico?
-- Note:
-- SUM(COUNT ) OVER () will return the sum of the grouped counts
-- thus giving the total count of the entire dataset
SELECT 
  country
  , ROUND(
      COUNT(userid):: numeric / SUM(
        COUNT(userid)
      ) OVER (), 
      3
    ) AS proportion 
FROM 
  users 
GROUP BY 
  country 
ORDER BY 
  country;

--Results:
--  country | proportion 
-- ---------+------------
--  CA      |      0.374
--  MX      |      0.222
--  US      |      0.405

-- Question 4: What are the proportions for each usertype
-- subscribers from the US, subscribers from Canada,...
-- non-subscribers from the US, non-subscibers from Canada...
-- Note:
-- SUM(COUNT ) OVER () will return the sum of the grouped counts
-- thus giving the total count of the entire dataset
SELECT 
  subscriber, 
  country, 
  ROUND(
    COUNT(userid):: numeric / SUM(
      COUNT(userid)
    ) OVER (), 
    3
  ) AS proportion 
FROM 
  users 
GROUP BY 
  subscriber, 
  country 
ORDER BY 
  subscriber, 
  country;

--Result:
--  subscriber | country | proportion 
-- ------------+---------+------------
--           0 | CA      |      0.311
--           0 | MX      |      0.185
--           0 | US      |      0.321
--           1 | CA      |      0.063
--           1 | MX      |      0.036
--           1 | US      |      0.084

-- Question 5: Which month(s) had the most and least points scored?
--    Which month(s) have the most and least participating players?
SELECT 
  DATE_TRUNC('month', event_date)::date AS month
  , SUM(points) AS total_points
  , COUNT(DISTINCT userid) AS total_users
FROM
  event_performance
GROUP BY
  DATE_TRUNC('month', event_date)::date
ORDER BY
  DATE_TRUNC('month', event_date)::date;
--Results:
--    month    | total_points | total_users 
-- ------------+--------------+-------------
--  2019-01-01 |        84380 |         441
--  2019-02-01 |       590556 |         601
--  2019-03-01 |       411009 |         612
--  2019-04-01 |       442820 |         613
--  2019-05-01 |       526904 |         617
--  2019-06-01 |       546596 |         638
--  2019-07-01 |       662819 |         652
--  2019-08-01 |      1812892 |         717
--  2019-09-01 |       810337 |         793
--  2019-10-01 |       370262 |         823
--  2019-11-01 |       585373 |         835
--  2019-12-01 |       414181 |         829

-- Question 6: How does player activity change from season to season?
WITH event_performance_seasons AS (
  SELECT 
    userid
    , event_date
    , hour
    , points
    , CASE WHEN EXTRACT(
        MONTH 
        FROM 
          event_date
      ) IN (3, 4, 5) THEN '0_spring' WHEN EXTRACT(
        MONTH 
        FROM 
          event_date
      ) IN (6, 7, 8) THEN '1_summer' WHEN EXTRACT(
        MONTH 
        FROM 
          event_date
      ) IN (9, 10, 11) THEN '2_fall' ELSE '3_winter' END AS season 
  FROM 
    event_performance
) 
SELECT 
  season, 
  hour, 
  SUM(points) AS tot_points 
FROM 
  event_performance_seasons 
GROUP BY 
  season, 
  hour 
ORDER BY 
  season, 
  hour;

--Results:
--   season  | hour | tot_points 
-- ----------+------+------------
--  0_spring |   18 |      65170
--  0_spring |   19 |     568171
--  0_spring |   20 |     747392
--  1_summer |   18 |     860831
--  1_summer |   19 |    1573432
--  1_summer |   20 |     588044
--  2_fall   |   16 |      26821
--  2_fall   |   17 |     234646
--  2_fall   |   18 |     575405
--  2_fall   |   19 |     663977
--  2_fall   |   20 |     265123
--  3_winter |   16 |      16661
--  3_winter |   17 |      27170
--  3_winter |   18 |     310770
--  3_winter |   19 |     498102
--  3_winter |   20 |     236414

-- Question 7: How does player activity change from event to event?
SELECT 
  DATE_TRUNC('day', event_date)::date AS day
  , SUM(points) AS total_points
FROM
  event_performance
GROUP BY
  DATE_TRUNC('day', event_date)::date
ORDER BY
  DATE_TRUNC('day', event_date)::date;
--Results (sample of rows):
--     day     | total_points 
-- ------------+--------------
--  2019-01-02 |        11908
--  2019-01-04 |        -3302
--  2019-01-07 |        15918
--  2019-01-08 |         -143
--  2019-01-15 |        30934
--     ...     |     ...     