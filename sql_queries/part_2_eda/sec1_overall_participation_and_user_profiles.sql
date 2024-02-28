-- Question 1: How many gaming events occured in the dataset?
--Note: Multiple rows in the event_performance table correspond
-- to the same gaming event, and DISTINCT is needed to identify how
--  many unique dates are in the dataset.  Since there is at most one
--  gaming event per day, the number of unique dates will reveal
-- how many gaming events occurred over the year.  
WITH row_summary_stats AS (
-- Calculating the number of gaming events
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
    )

SELECT 
  statistic
  , value
FROM
  row_summary_stats
ORDER BY
  num;

--Results:
--        statistic       |  value  
-- -----------------------+---------
--  num_gaming_events     |     147
--  gaming_event_yrly_pct | 40.2700


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


-- Question 5: What are the proportions of subscribers vs non-subscribers
-- from each country? 
SELECT subscriber
       , country
       , ROUND(COUNT(userid)::numeric / 
                      SUM(COUNT(userid)) OVER (PARTITION BY subscriber), 3) AS proportion
FROM users
GROUP BY subscriber, country
ORDER BY subscriber, country;
--Results:
--  subscriber | country | proportion 
-- ------------+---------+------------
--           0 | CA      |      0.380
--           0 | MX      |      0.227
--           0 | US      |      0.393
--           1 | CA      |      0.343
--           1 | MX      |      0.199
--           1 | US      |      0.458

-- Creating a subscriber vs a non-subscriber view to make future queries easier
CREATE VIEW event_performance_joined AS
  SELECT
      event_performance.userid
      , users.subscriber
      , users.country
      , event_performance.event_date
      , event_performance.hour
      , event_performance.points
    FROM
      event_performance
    LEFT JOIN
      users
      ON users.userid = event_performance.userid;

CREATE VIEW subscribers AS
  SELECT
    *
  FROM 
    event_performance_joined
  WHERE
    subscriber = 1;

CREATE VIEW non_subscribers AS
  SELECT
    *
  FROM 
    event_performance_joined
  WHERE
    subscriber = 0;


-- Question 6: How many of the 970 participating userids were subscribers
-- vs non-subscribers?

SELECT
  'non_subscribers' AS subscriber_status
  , COUNT(DISTINCT userid) AS "count"
  , ROUND(
      COUNT(DISTINCT userid) / (SELECT 
                                  COUNT(DISTINCT userid)
                                FROM
                                  event_performance_joined
                                  )::NUMERIC
                                   * 100
                                   , 2) AS "perc"
   FROM non_subscribers
UNION
SELECT
  'subscribers'
  , COUNT(DISTINCT userid)
  , ROUND(
      COUNT(DISTINCT userid) / (SELECT 
                                  COUNT(DISTINCT userid)
                                FROM
                                  event_performance_joined
                                  )::NUMERIC
                                   * 100
                                   , 2) AS "perc"
FROM subscribers;

--Results:
--  subscriber_status | count | perc  
-- -------------------+-------+-------
--  non_subscribers   |   795 | 81.12
--  subscribers       |   175 | 17.86


-- Question 7: What is the overall subscription rate?
SELECT
  ROUND(
    AVG(users.subscriber::numeric)
    * 100
    , 2) AS avg_subscription_rate
FROM 
  users;

--Results:
--  avg_subscription_rate 
-- -----------------------
--                  18.27