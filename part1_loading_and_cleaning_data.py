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
# # Loading and Cleaning Data

# %% [markdown]
# ## Creating staging tables to inspect and clean the data
# Create staging tables with generic data types so I can start looking through records in the table

# %%
sql_query = """
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
    """

exec_and_commit_query(sql_query, engine)

# %% [markdown]
# ## Loading dummy data from CSV files into tables

# %%
wd = os.getcwd()

sql_query = """ 
COPY users_staging(userid, attribute1, attribute2)
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
# ## Checking for NULL values

# %%
sql_query = """ 
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
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# No NULL values were found, so there shouldn't be any need to impute values or drop records with missing information.

# %% [markdown]
# ## Changing data types

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
#
# With that one problematic event_date dropped, I'm going to convert the remaining event_dates to the DATE type and to a more consistent format.  Most event_dates had 2 digit years, while some had four digit years.  This will standardize all event_dates to display the more descriptive four digit years.

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
  SELECT event_date
    FROM event_performance_staging
ORDER BY event_date
LIMIT 5;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# Inspecting the remaining dates, shows there's one date from 1999, which is before this company was founded.  Because there's no way to know what the true date for this entry is, it will have to be excluded from the analysis.  More on that later.

# %%
sql_query = """ 
  SELECT event_date
    FROM event_performance_staging
ORDER BY event_date DESC
LIMIT 5;
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# There's also one date from 2039, way into the future.  This date will have to be excluded from the analysis too.  I'll take care of that later.

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

# %% [markdown]
# A new error arises in the points column.  One of the entries is "732" and because of the quotiation marks, can't be converted to an INT.  Another entry also has a question mark in in that raises an error.  I'll remove these typos in a later step.

# %% [markdown]
# ### `user_id`

# %%
sql_query = """ 
  SELECT MIN(LENGTH(userid)) AS min_userid_length
       , MAX(LENGTH(userid)) AS max_userid_length
    FROM users_staging;
"""

sql_query_to_pandas_df(sql_query, engine)

# %%
sql_query = """ 
  SELECT MIN(LENGTH(userid)) AS min_userid_length
       , MAX(LENGTH(userid)) AS max_userid_length
    FROM event_performance_staging;
"""

sql_query_to_pandas_df(sql_query, engine)

# %%
sql_query = """ 
  SELECT userid
       , LENGTH(userid) AS userid_length
    FROM event_performance_staging
    WHERE LENGTH(userid) = (SELECT MAX(LENGTH(userid)) 
                              FROM event_performance_staging);
"""

sql_query_to_pandas_df(sql_query, engine)

# %% [markdown]
# These two entries have a userid that's 37 characters long, while all of the others in both tables have just 36 characters.  The second userid clearly has a quotation mark at the end and must be a typo that needs to be removed.  The problem with the first userid isn't quite as obvious, but upon closer inspection has a typo because there's a space character at the end.  Both of these typos will be removed shortly.

# %% [markdown]
# ## Moving data to new, clean tables

# %% [markdown]
# ### `event_performance`
#
# 1. userid is a 36 character string.  
# 	* Note: All userids in this table were 36 characters, 
# 	except for two user ids that were 37 characters.
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
        										   --the company was founded.
    """

exec_and_commit_query(sql_query, engine)

# %% [markdown]
# ### `users`
#
# users table:
# 1. userid is meant to be a unique 36 character string.
# 	* For this reason, I set userid to the PRIMARY KEY
# 2. attribute1 is an integer that can only take on values of 0 or 1
# 3. attribute2 is a single character string that only takes on 'A', 'B', or 'C'
#
# No cleaning was needed for users data.  This query simply creates a new table with more explcit data types and constraints, then copies the data from users_staging to the users table.

# %%
sql_query = """
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
