DROP TABLE IF EXISTS users;

-- Create new users table with constraints
CREATE TABLE 
  users (userid VARCHAR(36) NOT NULL
         , subscriber int NOT NULL
         , country VARCHAR(2) NOT NULL
         , PRIMARY KEY (userid)
         , CONSTRAINT valid_subscriber 
             CHECK (subscriber IN (0, 1))
         , CONSTRAINT valid_country
             CHECK (country IN ('CA', 'US', 'MX'))
          );

-- No cleaning is needed for the users data.  This simply
-- transfers the data from the user_staging table into the
-- users table
INSERT INTO users(userid, subscriber, country)
SELECT userid
       , subscriber::int
       , country
FROM users_staging;


DROP TABLE IF EXISTS event_performance;

-- Create new event_performance table with constraints
CREATE TABLE event_performance (userid VARCHAR(36) NOT NULL
                                , event_date date NOT NULL
                                , hour int NOT NULL
                                , points int NOT NULL
                                , CONSTRAINT valid_hour
                                    CHECK (hour >= 0 AND hour <= 23)
                                , CONSTRAINT valid_event_date
                                    CHECK (event_date <= CURRENT_DATE 
                                             AND event_date >= '2013-01-01')
                                );

-- This query filters out dates that weren't possible
-- and replaces unwanted characters like question marks
-- and quotation marks from the userid and points columns
INSERT INTO event_performance(userid, event_date, hour, points) 
SELECT REGEXP_REPLACE(userid, '[" ]', '', 'gi')
       , event_date
       , hour::int
       , REGEXP_REPLACE(points, '["?]', '', 'gi')::int
FROM event_performance_staging
WHERE
  event_date <= '2023-07-13' 
  AND event_date >= '2013-01-01';