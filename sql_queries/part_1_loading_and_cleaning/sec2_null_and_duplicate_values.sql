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

-- Question 3: Are there any duplicate values in the users table?
SELECT
  userid AS duplicate_ids
  , COUNT(userid) AS num_records
FROM
  users_staging
GROUP BY
  userid
HAVING 
  COUNT(userid) > 1;

--Results:
--  duplicate_ids | num_records 
-- ---------------+-------------
-- (0 rows)

-- Question 4: Are there any duplicate values in the event_performance table?
SELECT
  userid
  , event_date
  , hour
  , points
  , COUNT(*) AS num_records
FROM
  event_performance_staging
GROUP BY
  userid
  , event_date
  , hour
  , points
HAVING
  COUNT(*) > 1;

-- Results:
--  userid | event_date | hour | points | num_records 
-- --------+------------+------+--------+-------------
-- (0 rows)