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