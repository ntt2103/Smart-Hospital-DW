SELECT a.department_id,a.value AS checkins,b.value AS results
FROM mart.rt01_checkins_last_15m(%(as_of)s) a
JOIN mart.rt02_lab_results_last_15m(%(as_of)s) b USING(department_id)
ORDER BY 1;
