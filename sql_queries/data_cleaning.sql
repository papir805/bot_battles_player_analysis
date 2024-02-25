-- Question 1: Are there any null values in the users table?
SELECT 
  userid
  , subscriber
  , country
FROM 
  users_staging
WHERE
  userid IS NULL
  OR subscriber IS NULL
  OR country IS NULL;

-- Results:
--  userid | subscriber | country 
-- --------+------------+---------
-- (0 rows)

-- Question 2: Are there any null values in the event_performance table?
SELECT 
  userid
  , event_date
  , hour
  , points
FROM 
  event_performance_staging
WHERE
  userid IS NULL
  OR event_date IS NULL
  OR hour IS NULL
  OR points IS NULL;

-- Results:
--  userid | event_date | hour | points 
-- --------+------------+------+--------
-- (0 rows)