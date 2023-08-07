# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Preliminary Steps

# %% [markdown]
# ## Imports

# %%
from dotenv import load_dotenv

import os

import traceback

from sql_query_helper_funcs import exec_and_commit_query, sql_query_to_pandas_df

import pandas as pd

from sqlalchemy import create_engine

# %% [markdown]
# ## Connecting to local db

# %%
load_dotenv()

# %%
db_user = os.environ.get('USER_NAME')
db_pass = os.environ.get('PASS')
db_ip = os.environ.get('IP_ADDRESS')
db_port = os.environ.get('PORT')
db_name = os.environ.get('DB_NAME')

engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_ip}:{db_port}/{db_name}')

# %% [markdown]
# # Loading Data

# %% [markdown]
# ## Creating staging tables to inspect and clean the data
# Create staging tables with generic data types so I can start looking through records in the table

# %%
sql_query = """
    DROP TABLE IF EXISTS users_staging;

    CREATE TABLE users_staging (
             userid text,
         subscriber int,
         category text
        );

    DROP TABLE IF EXISTS event_performance_staging;

    CREATE TABLE event_performance_staging (
            userid text,
        event_date text,
              hour int,
            points text
        );
    """

exec_and_commit_query(sql_query, engine)

# %% [markdown]
# ## Loading dummy data from CSV files into tables

# %%
wd = os.getcwd()

sql_query = """ 
COPY users_staging(userid, subscriber, category)
FROM '{working_dir}/data/users.csv'
DELIMITER ','
CSV HEADER;

COPY event_performance_staging(userid, event_date, hour, points)
FROM '{working_dir}/data/event_performance.csv'
DELIMITER ','
CSV HEADER;
""".format(working_dir = wd)

exec_and_commit_query(sql_query, engine)

# %% [markdown]
# # Checking for NULL Values

# %%
sql_query = """ 
SELECT *
  FROM users_staging
 WHERE userid IS NULL
    OR subscriber IS NULL
    OR category IS NULL;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# No Null values were found in the users_staging table.

# %%
sql_query = """ 
SELECT *
  FROM event_performance_staging
 WHERE userid IS NULL
    OR event_date IS NULL
    OR hour IS NULL
    OR points IS NULL;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# No NULL values were found in event_performance_staging either.  As no NULL values exist in either table, there shouldn't be any need to impute values or drop records with missing information.

# %% [markdown]
# # Checking for Duplicate Values

# %%
sql_query = """ 
  SELECT userid
       , COUNT(userid)
    FROM users_staging
    GROUP BY userid
    HAVING COUNT(userid) > 1;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# No duplicate values found in the users_staging table.

# %%
sql_query = """ 
  SELECT userid, event_date, hour, points, COUNT(*)
    FROM event_performance_staging
    GROUP BY userid, event_date, hour, points
    HAVING COUNT(*) > 1;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# No duplicate values found in the event_performance_staging table.

# %% [markdown]
# # Cleaning Data

# %% [markdown]
# ## Cleaning `users_staging`

# %% [markdown]
# ### `userid`

# %%
sql_query = """ 
  SELECT LENGTH(userid) AS userid_length
       , COUNT(*) AS frequency
    FROM users_staging
    GROUP BY LENGTH(userid);
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# All userids are 36 characters, indicating userids have a standardized length.  Visual inspection shows the userids contain a mixture of alphanumeric characters, so I'll make them VARCHAR(36) when moving the cleaned data to the new table.

# %% [markdown]
# ### `subscriber`

# %%
sql_query = """ 
  SELECT DISTINCT(subscriber::int)
    FROM users_staging;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# Because subscriber is a binary variable, I would like it to be the <i>int</i> data type.  It converts to <i>int</i> with no problems and all values in the dataset are either 0 or 1, as they're supposed to be.  Everything looks good here.

# %% [markdown]
# ### `category`

# %%
sql_query = """ 
  SELECT DISTINCT(category::VARCHAR(1))
    FROM users_staging;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# Because category is a single character string representing which category a user falls into, I would like it to be the <i>VARCHAR(1)</i> data type.  It converts to <i>VARCHAR(1)</i> with no problems and all values in the dataset are either 'A', 'B', or 'C', as they're supposed to be.  Everything looks good here too.

# %% [markdown]
# ## Cleaning `event_performance`

# %% [markdown]
# ### `userid`

# %%
sql_query = """ 
  SELECT LENGTH(userid) AS userid_length
       , COUNT(*) AS frequency
    FROM event_performance_staging
    GROUP BY LENGTH(userid);
"""

sql_query_to_pandas_df(sql_query, engine)


# %% [markdown]
# It was noted earlier that all userids in the users_staging table had 36 characters, however 2 userids have 37 characters in this table. This has to be a mistake and I isolate those rows to inspect the userids and figure out why.

# %%
def quote(s):
    rep_dict = {' ': '\s'}
    return "".join(rep_dict.get(c, c) for c in s)


# %%
sql_query = """ 
  SELECT userid
       , LENGTH(userid) AS userid_length
    FROM event_performance_staging
    WHERE LENGTH(userid) = (SELECT MAX(LENGTH(userid)) 
                              FROM event_performance_staging);
"""

long_userids = sql_query_to_pandas_df(sql_query, engine)

long_userids['userid'] = long_userids['userid'].apply(quote)

long_userids

# %% [markdown]
# These two entries have a userid that's 37 characters long, while all of the others in both tables have just 36 characters.  The second userid clearly has a quotation mark at the end, however, the problem with the first userid isn't quite as obvious. Close inspection reveals there's a whitespace character at the end of the userid, and I use a function to convert that character to "\s" to make it easier to see. Both of these must be typos and will be removed shortly.

# %% [markdown]
# ### `event_date`

# %%
sql_query = """ 
SELECT event_date::date
  FROM event_performance_staging;
"""

try:
    sql_query_to_pandas_df(sql_query, engine)
except Exception as e:
    traceback.print_exc(limit=1)
    exit(1)

# %% [markdown]
# The error is caused by one entry in the event_date column that's set to "19/24/2019".  Since there's no way to identify whether this date is meant to be "1/24/2019" or "9/24/2019" or perhaps something else entirely, the safest option is to drop it.  Dropping one entry in a dataset this size shouldn't make much of a difference.

# %%
sql_query = """
DELETE FROM event_performance_staging
WHERE event_date = '19/24/2019';
    """

exec_and_commit_query(sql_query, engine)

# %% [markdown]
# With that one problematic event_date dropped, I'm going to convert the remaining event_dates to the DATE type and to a more consistent format.  Most event_dates had 2 digit years in the MM/DD/YY format, while some had four digit years in a different format.  Using the to_date function will standardize all event_dates to display the more descriptive four digit years in the YYYY-MM-DD format.

# %%
sql_query = """
ALTER TABLE event_performance_staging
	ALTER COLUMN event_date
	TYPE DATE
	USING (to_date(event_date, 'MM/DD/YY'));
    """

exec_and_commit_query(sql_query, engine)

# %%
sql_query = """ 
  SELECT event_date AS earliest_dates
    FROM event_performance_staging
ORDER BY event_date
LIMIT 5;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# Inspecting the remaining dates, there's one date from 1999, which is before this company was founded.  Because there's no way to know what the true date for this entry is, it will have to be excluded from the analysis.  I'll do that later when moving the cleaned data to the new tables.

# %%
sql_query = """ 
  SELECT event_date AS latest_dates
    FROM event_performance_staging
ORDER BY event_date DESC
LIMIT 5;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# There's also one date from 2039, way into the future.  This date will have to be excluded from the analysis too.  I'll also take care of that later when moving the cleaned data to the new tables.

# %% [markdown]
# ### `hour`

# %%
sql_query = """ 
  SELECT DISTINCT(hour::int)
    FROM event_performance_staging
    ORDER BY 1;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# Because hour represents an hour of the day, I would like to convert it to the int data type in the new table. It converts just fine and the values appear to be using military time, ranging from 16:00-20:00, or 4:00pm-8:00pm.

# %% [markdown]
# ### `points`

# %%
sql_query = """ 
SELECT points::int
  FROM event_performance_staging;
"""

try:
    sql_query_to_pandas_df(sql_query, engine)
except Exception as e:
    traceback.print_exc(limit=1)
    exit(1)

# %%
sql_query = """ 
SELECT points
FROM event_performance_staging
WHERE points ~ '^.*[^A-Za-z0-9 .-].*$';
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# A new error arises in the points column.  One of the entries is "732" and because of the quotiation marks, can't be converted to an INT.  Another entry also has a question mark in in that raises an error.  I'll remove these typos in a later step.

# %% [markdown]
# ## Moving data to new, clean tables

# %% [markdown]
# ### `users`
#
# Based on what was seen earlier, I plan to set up the following schema for a newly defined users table:
# 1. userid is meant to be a unique 36 character string.
# 	* For this reason, I set userid to be a PRIMARY KEY
# 2. subscriber is an integer that can only take on values of 0 or 1
# 3. category is a single character string that only takes on 'A', 'B', or 'C'
#
# No cleaning was really needed for users data.  The query below simply creates a new table with more explcit data types and introduces constraints to ensure all rows get entered with the correct values.  Once the new table is created, the query then copies the data from users_staging table to the users table.

# %%
sql_query = """
DROP TABLE IF EXISTS users;

CREATE TABLE users (
	  userid VARCHAR(36) NOT NULL,
  subscriber int NOT NULL,
  category VARCHAR(1) NOT NULL,
 PRIMARY KEY (userid),
  CONSTRAINT valid_subscriber CHECK (subscriber IN (0, 1)),
  CONSTRAINT valid_category CHECK (category IN ('A', 'B', 'C'))
	);

 INSERT INTO users(userid, subscriber, category)
      SELECT userid
           , subscriber
           , category
        FROM users_staging;
    """

exec_and_commit_query(sql_query, engine)

# %% [markdown]
# ### `event_performance`
#
# Here's the schema for the new event_performance table:
# 1. userid is a 36 character string.  
# 	* Note: All userids in this table were 36 characters, 
# 	except for two that were 37 characters.
# 	One of these userids had double quotes at the end and the other had
# 	a space at the end, both of which I assume are typos and are
# 	fixed upon transferring to the new, clean table.
# 2. event_date is DATE type, with dates occurring during years
# only subsequent to the company's creation and before the analysis was performed.
# 	* Note: I found one date that occurs in 2039 and
# 	another from 1999 and end up excluding both in the analysis.
# 3. hour is an integer between 0 and 23 inclusive
# 4. points is an integer
#
# This query creates a new table to hold the cleaned events_performance data and defines more explicit data types.  The quotation mark and question mark are removed from `points`, the space and quotation mark are removed from `userid`, and dates prior to the founding of the company, or dates after the analysis was performed are excluded. 

# %%
sql_query = """
DROP TABLE IF EXISTS event_performance;

CREATE TABLE event_performance (
	    userid VARCHAR(36) NOT NULL,
	event_date DATE NOT NULL,
	      hour int NOT NULL,
	    points int NOT NULL,
	CONSTRAINT valid_hour CHECK (hour >= 0 AND hour <= 23),
    CONSTRAINT valid_event_date CHECK (event_date <= CURRENT_DATE AND 
                                       event_date >= '2013-01-01')
	);

INSERT INTO event_performance(userid, event_date, hour, points) 
     SELECT REGEXP_REPLACE(userid, '[" ]', '', 'gi')
          , event_date
          , hour
          , REGEXP_REPLACE(points, '["?]', '', 'gi')::int
       FROM event_performance_staging
      WHERE event_date <= '2023-07-13'  --Date isn't from the future
        AND event_date >= '2013-01-01'; --Date is from after
        										   --the company was founded.
    """

exec_and_commit_query(sql_query, engine)

# %% [markdown]
# ## Exporting cleaned data to backup CSV file

# %%
sql_query = """
COPY event_performance TO '{working_dir}/data/clean/event_performance_clean.csv' 
                     WITH DELIMITER ',' 
                      CSV HEADER;
    """.format(working_dir=wd)

exec_and_commit_query(sql_query, engine)

# %% [markdown]
# # The End
