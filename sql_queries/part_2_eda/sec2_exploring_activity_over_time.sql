-- Question 1: Which month(s) had the most and least points scored?
--    Which month(s) have the most and least participating players?
SELECT DATE_TRUNC('month', event_date)::date AS month
       , SUM(points) AS total_points
       , COUNT(DISTINCT userid) AS total_users
FROM event_performance
GROUP BY DATE_TRUNC('month', event_date)::date
ORDER BY DATE_TRUNC('month', event_date)::date;
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

-- Question 2: How does player activity change from season to season?
WITH event_performance_seasons AS (
  SELECT userid
         , event_date
         , hour
         , points
         , CASE 
             WHEN EXTRACT(
                    MONTH FROM event_date
                    ) IN (3, 4, 5) THEN '0_spring'
             WHEN EXTRACT(
                    MONTH FROM event_date
                    ) IN (6, 7, 8) THEN '1_summer'
             WHEN EXTRACT(
                    MONTH FROM event_date
                    ) IN (9, 10, 11) THEN '2_fall' 
             ELSE '3_winter' 
           END AS season 
  FROM event_performance
) 

SELECT season
       , hour
       , SUM(points) AS tot_points 
FROM event_performance_seasons 
GROUP BY season, hour 
ORDER BY season, hour;

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

-- Question 3: How does player activity change from event to event?
SELECT DATE_TRUNC('day', event_date)::date AS day
       , SUM(points) AS total_points
FROM event_performance
GROUP BY DATE_TRUNC('day', event_date)::date
ORDER BY DATE_TRUNC('day', event_date)::date;
--Results (sample of rows):
--     day     | total_points 
-- ------------+--------------
--  2019-01-02 |        11908
--  2019-01-04 |        -3302
--  2019-01-07 |        15918
--  2019-01-08 |         -143
--  2019-01-15 |        30934
--     ...     |     ...     

SELECT country
     , COUNT(hour) AS cnt
FROM users
LEFT JOIN event_performance
ON users.userid = event_performance.userid
GROUP BY country;

SELECT country
     , subscriber
     , ROUND(COUNT(userid) / SUM(COUNT(userid)) OVER () * 100, 2) AS pct
FROM users
GROUP BY country, subscriber
ORDER BY 1, 2, 3 DESC;

SELECT country
     , subscriber
     , ROUND(COUNT(userid) / SUM(COUNT(userid)) OVER (PARTITION BY country) * 100, 2) AS pct
FROM users
GROUP BY country, subscriber
ORDER BY 1, 2, 3 DESC;