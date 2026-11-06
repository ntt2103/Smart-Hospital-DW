SELECT result_date_key,lab_test_type_sk,count(*) AS completed
FROM dw.fact_lab_order WHERE is_completed_eligible AND department_sk=%(department)s
AND result_date_key BETWEEN %(start_key)s AND %(end_key)s
GROUP BY 1,2 ORDER BY 1,2;
