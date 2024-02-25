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