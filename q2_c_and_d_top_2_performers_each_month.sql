\c ohmconnect

WITH points_rankings AS (
	  SELECT userid
	       , EXTRACT(MONTH FROM event_date) AS month
	       , SUM(points) AS points_earned
	       , DENSE_RANK() OVER (PARTITION BY EXTRACT(MONTH FROM event_date) 
	     					  ORDER BY SUM(points) DESC
	     					  ) AS ranking
	    FROM event_performance
	GROUP BY userid, EXTRACT(MONTH FROM event_date)
	)

  SELECT userid
       , month
       , points_earned
       , ranking
    FROM points_rankings
   WHERE ranking <= 2
ORDER BY month, ranking;