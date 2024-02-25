-- Loading users.csv into users_staging table
DROP TABLE IF EXISTS 
  users_staging;
CREATE TABLE 
  users_staging (userid text
                 , subscriber text
                 , country text);
COPY
  users_staging(userid, subscriber, country)
FROM
  '{working_dir}/data/users.csv'
DELIMITER
  ','
CSV HEADER;

-- Loading event_performance.csv into event_performance_staging table
DROP TABLE IF EXISTS
  event_performance_staging;
CREATE TABLE
  event_performance_staging (userid text
                             , event_date text
                             , hour text
                             , points text);
COPY
  event_performance_staging(userid, event_date, hour, points)
FROM
  '{working_dir}/data/event_performance.csv'
DELIMITER
  ','
CSV HEADER;


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