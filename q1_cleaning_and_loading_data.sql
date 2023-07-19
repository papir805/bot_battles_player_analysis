\c ohmconnect

/*
Creating staging tables to inspect and clean the data
*/

DROP TABLE IF EXISTS users_staging;

CREATE TABLE users_staging (
	     userid text,
	 attribute1 int,
	 attribute2 text
	);

DROP TABLE IF EXISTS event_performance_staging;

CREATE TABLE event_performance_staging (
	    userid text,
	event_date text,
	      hour int,
	    points text
	);

/*
Copying CSVs to staging tables
*/

COPY users_staging(userid, attribute1, attribute2)
FROM '/Users/rancher/Google Drive/Coding/Interviews/OhmConnect/take_home_assignment/app_and_user_behavior_report/data/users.csv'
DELIMITER ','
CSV HEADER;

COPY event_performance_staging(userid, event_date, hour, points)
FROM '/Users/rancher/Google Drive/Coding/Interviews/OhmConnect/take_home_assignment/app_and_user_behavior_report/data/event_performance.csv'
DELIMITER ','
CSV HEADER;

/*
Checking for Null Values
*/

SELECT *
  FROM event_performance_staging
 WHERE userid IS NULL
    OR event_date IS NULL
    OR hour IS NULL
    OR points IS NULL;

SELECT *
  FROM users_staging
 WHERE userid IS NULL
    OR attribute1 IS NULL
    OR attribute2 IS NULL;

/*
Attempting to convert event_date to DATE type
*/

SELECT event_date::date
  FROM event_performance_staging;

/*
Dropping problematic values

I'd like to convert event_date column to a DATE type, but the
date below, 19/24/2019, would give me an error when doing so.  
Because it's unclear whether the date is meant to be 1/24/2019, 
9/24/2019, or perhaps something else, I decided to drop this 
entry from the dataset.Dropping one observation from a dataset 
this size shouldn't matter too much.
*/

DELETE FROM event_performance_staging
WHERE event_date = '19/24/2019';

/*
Converting event_date to DATE type and standardizing date
format.

Most dates had 2 digit years, while some had four digit years.
I'm going to convert them all to a consistent format, choosing
the more descriptive four digit years.
*/

ALTER TABLE event_performance_staging
	ALTER COLUMN event_date
	TYPE DATE
	USING (to_date(event_date, 'MM/DD/YY'));

/*
Inspecting remaining dates
*/

  SELECT event_date
    FROM event_performance_staging
ORDER BY event_date
LIMIT 10;

  SELECT event_date
    FROM event_performance_staging
ORDER BY event_date DESC
LIMIT 10;

/*
Attempting to convert points to int type
*/

SELECT points::int
  FROM event_performance_staging;

/*
Excluding new, problematic values and then moving
events_performance_staging to new, clean table.

There were two values in the points column that prevented 
converting the points column to an integer.  One entry was
"732" and the other was 2006?.  In order to clean them,
I drop the quotation marks and question marks, when filling
in the new, clean table.

Also, I exclude the dates corresponding to 1999 and 2039.

Assumptions

event performance table:
1. userid is a 36 character string.  
	Note: All userids in this table were 36 characters, 
	except for two user ids that were 37 characters.
	One of these userids had double quotes at the end and the other had
	a space at the end, both of which I assume are typos and are
	fixed upon transferring to the new, clean table.
2. event_date is DATE type, with dates occurring during years
only subsequent to OhmConnect's creation and before the take
home assignment was given to me.
	Note: I found one date that occurs in 2039 and
	another from 1999 and end up excluding both in the analysis.
3. hour is an integer between 0 and 23 inclusive
4. points is an integer
*/

DROP TABLE IF EXISTS event_performance;

CREATE TABLE event_performance (
	    userid VARCHAR(36) NOT NULL,
	event_date DATE NOT NULL,
	      hour int NOT NULL,
	    points int NOT NULL,
	CONSTRAINT valid_hour CHECK (hour >= 0 AND hour <= 23)
	);

INSERT INTO event_performance(userid, event_date, hour, points) 
     SELECT REGEXP_REPLACE(userid, '[" ]', '', 'gi')
          , event_date
          , hour
          , REGEXP_REPLACE(points, '["?]', '', 'gi')::int
       FROM event_performance_staging
      WHERE event_date <= '2023-07-13' --Date isn't from the future
        AND EXTRACT(YEAR FROM event_date) >= 2013; --Date is from after
        																					 --OhmConnect was founded.

/*
Moving users_staging into new users table

No cleaning was needed for the users table.  I'm simply going 
to move the data into a new table and define more explicit data 
types and constraints.

Assumptions

users table:
1. userid is meant to be a unique 36 character string
	For this reason, I set userid to the PRIMARY KEY
2. attribute1 is an integer that can only take on values of 0 or 1
3. attribute2 is a single character string that only takes on 'A', 'B', or 'C'
*/

DROP TABLE IF EXISTS users;

CREATE TABLE users (
	  userid VARCHAR(36) NOT NULL,
  attribute1 int NOT NULL,
  attribute2 VARCHAR(1) NOT NULL,
 PRIMARY KEY (userid),
  CONSTRAINT valid_attribute1 CHECK (attribute1 IN (0, 1)),
  CONSTRAINT valid_attribute2 CHECK (attribute2 IN ('A', 'B', 'C'))
	);

 INSERT INTO users(userid, attribute1, attribute2)
      SELECT userid
           , attribute1
           , attribute2
        FROM users_staging;

/*
Exporting cleaned copy of event_performance table to local drive,
just to have a backup of the cleaned CSV.
*/

COPY event_performance TO '/Users/rancher/Google Drive/Coding/Interviews/OhmConnect/take_home_assignment/app_and_user_behavior_report/data/clean/event_performance_clean.csv' 
                     WITH DELIMITER ',' 
                      CSV HEADER;