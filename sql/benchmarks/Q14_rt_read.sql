SELECT department_sk,checkins_last_15m,lab_results_last_15m
FROM mart.realtime_department_snapshot WHERE as_of=(SELECT max(as_of) FROM mart.realtime_department_snapshot WHERE as_of<=%(as_of)s)
ORDER BY department_sk;
