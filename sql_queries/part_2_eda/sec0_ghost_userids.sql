-- Question 1: How many unique userids in the event_performance table
-- match with a userid in the users table
SELECT COUNT(DISTINCT users.userid) AS num_unique_users_tbl
       , COUNT(DISTINCT event_performance.userid) AS num_unique_event_perf_tbl
FROM event_performance
LEFT JOIN users
  ON users.userid = event_performance.userid;

--Results:
--  num_unique_users_tbl | num_unique_event_perf_tbl 
-- ----------------------+---------------------------
--                   970 |                       980

-- Question 2: Which userids show up in event_performance but don't show up in 
-- the users table?
SELECT DISTINCT(event_performance.userid) AS ghost_userids
FROM event_performance
LEFT JOIN
  users 
  ON users.userid = event_performance.userid
WHERE users.userid IS NULL;

--Results:
--             ghost_userids             
-- --------------------------------------
--  3eca708c-2767-44a7-b1a9-3a1137eec60e
--  47d61edb-1f89-4e91-ba08-f05425b07ac4
--  8da6eb1a-7642-48d6-92ac-989916dc241e
--  ab4e376f-37c7-4694-aede-7d52419fc1ac
--  ae11ee98-268f-4136-9997-68324c8e745d
--  b2c0a4a2-9bdc-4f29-99ee-0c5e4a81c6ac
--  b8aae0f6-21bb-496c-b9fd-19299a8a503e
--  cc3e6f9e-2ab1-4039-a80f-ad8709f26ed5
--  e8d027be-8b65-491b-9920-e513c78b34b3
--  f01dc5e7-b15a-4c70-a2bc-3388fa11a77b

-- Question 3: How many entries in event_performance come from
-- these ghost userids?
SELECT COUNT(*) AS ghost_userid_num_entries
FROM event_performance
LEFT JOIN users
  ON users.userid = event_performance.userid
WHERE users.userid IS NULL;

--Results:
--  ghost_userid_num_entries 
-- --------------------------
--                       417

SELECT COUNT(*)
FROM event_performance;