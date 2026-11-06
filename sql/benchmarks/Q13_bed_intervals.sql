SELECT bed_assignment_id,encounter_id,assigned_at,released_at
FROM dw.fact_bed_assignment a JOIN dw.dim_bed b USING(bed_sk)
WHERE b.bed_id=%(bed)s AND a.assigned_at<((%(end)s::date+1)::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')
AND coalesce(a.released_at,'infinity')>(%(start)s::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')
ORDER BY a.assigned_at,a.bed_assignment_id;
