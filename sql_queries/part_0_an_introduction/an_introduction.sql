-- Question 1: How many registered players are there?
SELECT 
  COUNT(DISTINCT userid) AS num_players
FROM 
  users;

-- Results:
--num_players
-------------
--      1100

-- Question 2: How many registered players participated in a gaming 
-- event last year?
SELECT
  COUNT(DISTINCT userid) AS num_participating_players
FROM
  event_performance;

-- Results:
--num_participating_players 
-- ---------------------------
--                        980

-- Question 3: How many gaming events were there?
SELECT
  COUNT(DISTINCT event_date) AS num_gaming_events
FROM 
  event_performance;

-- Results
--num_gaming_events
-------------------
--            147