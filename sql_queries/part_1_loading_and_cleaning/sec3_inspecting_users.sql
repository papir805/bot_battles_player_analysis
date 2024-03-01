-- Question 1: Are all userids the same length?
SELECT LENGTH(userid) AS userid_length
       , COUNT(*) AS frequency
FROM users_staging
GROUP BY LENGTH(userid);
--Results:
--  userid_length | frequency 
-- ---------------+-----------
--             36 |      1100

-- Question 2: What are the distinct values for subscriber?
SELECT DISTINCT(subscriber::int)
FROM users_staging;
--Results:
--  subscriber 
-- ------------
--           0
--           1

-- Question 3: What countries are does the game have players in?
SELECT DISTINCT(country::VARCHAR(2))
FROM users_staging;
--Results:
--  country 
-- ---------
--  US
--  MX
--  CA

