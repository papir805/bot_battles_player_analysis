-- Question 1: Are userids in the event_performance table also 36 alphanumeric
-- characters like in the userid table?
SELECT 
  LENGTH(userid) AS userid_length
  , COUNT(*) AS frequency
FROM
  event_performance_staging
GROUP BY
  LENGTH(userid);

-- Results:
--  userid_length | frequency 
-- ---------------+-----------
--             37 |         2
--             36 |     37569

-- Question 2: What do the 2 userids that are 37 characters long look like?
SELECT
  userid
  , LENGTH(userid) AS userid_length
FROM
  event_performance_staging
WHERE
  LENGTH(userid) = (SELECT MAX(LENGTH(userid)) 
                    FROM event_performance_staging
                    );

--Results:
--                 userid                 | userid_length 
-- ---------------------------------------+---------------
--  4297f22d-1889-4b5b-80bb-cdaa6a8809bd  |            37
--  5ddbc2c6-6e9b-4f0b-af64-487074332862" |            37

-- Question 3: Are there any invalid dates?
-- Step 1: Drop any invalid dates
DELETE FROM
  event_performance_staging
WHERE
  event_date = '19/24/2019';

-- Step 2: Convert all dates to MM/DD/YY format
ALTER TABLE 
  event_performance_staging
ALTER COLUMN
  event_date
TYPE 
  DATE
USING 
  (to_date(event_date, 'MM/DD/YY'));

-- Step 3: Identify any entries in the dataset that are impossible.  Dates from
-- before the company was formed or dates from the future which haven't
-- happened yet
WITH first_dates AS (
  SELECT
    event_date
    , ROW_NUMBER() OVER (ORDER BY event_DATE ASC) AS rn
  FROM
    event_performance_staging
  LIMIT
    3
  ),

last_dates AS (
  SELECT
    event_date
    , ROW_NUMBER() OVER (ORDER BY event_DATE DESC) AS rn
  FROM
    event_performance_staging
  LIMIT
    3
)

SELECT
  fd.event_date AS earliest_dates
  , ld.event_date AS latest_dates
FROM
  first_dates AS fd
INNER JOIN
  last_dates as ld
  ON fd.rn = ld.rn
ORDER BY
  fd.event_date ASC
  , ld.event_date ASC;

-- Results:
--  earliest_dates | latest_dates 
-- ----------------+--------------
--  1999-03-22     | 2039-08-02
--  2019-01-02     | 2019-12-27
--  2019-01-02     | 2019-12-27

-- Question 4: What are the distinct hours of the day this game has been played?
SELECT 
  DISTINCT(hour::int)
FROM
  event_performance_staging
ORDER BY
  1; 

--Results:
--  hour 
-- ------
--    16
--    17
--    18
--    19
--    20

-- Question 5: Do the point totals have any non-numeric characters?
SELECT
  points
FROM
  event_performance_staging
WHERE
  points ~ '^.*[^A-Za-z0-9 .-].*$';

-- Results:
--  points 
-- --------
--  "732"
--  2006??