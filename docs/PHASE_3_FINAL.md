# Phase 3 — Batch warehouse và historical BI

**Hoàn tất ngày 23/10/2026.** Dữ liệu thực trong warehouse, 9 KPI lịch sử và ba dashboard Metabase đã vận hành qua API/DAG; đã qua gate batch trước khi triển khai streaming.

## Đã làm gì, để làm gì và kết quả

| Công việc | Mục đích | Kết quả đã kiểm tra |
|---|---|---|
| Incremental source→raw→stg với cursor updated_at/PK, upper bound cố định và lookback120 phút | Bắt inserts/updates, chạy lại an toàn | Initial151.433 rows; lần kiểm incremental sau mutation10.425 rows; raw/stg/source đối soát10 bảng cả keys và payload |
| Transaction merge và watermark, run audit/quarantine | Không công bố dữ liệu/checkpoint khi job lỗi | Fault injection rollback giữ nguyên watermark; lỗi DQ liên bảng bị chặn, payload sai được quarantine, sửa nguồn rồi chạy lại thành công |
| 6 dimensions, 7 fact definitions, shared SK và observed doctor SCD2 | Tách grain và giữ lịch sử có bằng chứng | Nạp50.000 appointment facts,50.000 OP,4.000 IP,4.795 assignments,32.000 lab; doctor đổi inactive/active mở phiên bản, facts quá khứ không bịa lịch sử doctor |
| Snapshot giường00:00 Asia/Ho_Chi_Minh theo history/assignment [start,end) | Occupancy đúng thời điểm và loại maintenance | 88.080 snapshots tại lần nghiệm thu; source correction released_at làm snapshot cũ đổi đúng; backfill giới hạn ngày và rerun không nhân grain |
| Daily marts và9 KPI | Phục vụ phân tích ngày/khoa với date roles đúng | 28 SQL fixture assertions đạt, phủ OP01–03/IP01–03/LAB01–03; NULL denominator, waiting/TAT lỗi và frozen target được kiểm chứng |
| Airflow LocalExecutor DAG hospital_batch | Điều phối batch60 phút, retry/timeout và backfill | Cron0 * * * *, max_active_runs1, retries2, timeout45 phút; run thật qua API sau injected failure đã success ở try3 |
| Ba dashboard với saved SQL questions, date/khoa/snapshot filters | Đọc kết quả DW bằng reporter | 22 questions chạy;11 đối chiếu scalar/date-filter giữa Metabase và SQL đạt; bộ lọc khoa được thử trên questions |

## Dashboard

- [Executive Overview](http://localhost:13000/dashboard/2): OP01/IP01/IP02/LAB01 và xu hướng.
- [Outpatient Operations](http://localhost:13000/dashboard/3): OP01/OP02/OP03 và daily trends.
- [Inpatient & Bed Management](http://localhost:13000/dashboard/4): IP01/IP02/IP03, admission/LOS và giường theo snapshot.

Historical filters mặc định22/10/2025–21/10/2026 để phù hợp snapshot synthetic; occupancy chọn một ngày lúc00:00. Tỷ lệ/trung bình cộng thành phần trước rồi mới chia, không AVG các tỷ lệ ngày/khoa. BI chỉ dùng mart/safe views, không OLTP/raw.

## Kiểm thử và giới hạn thực tế

Evidence JSON nằm trong artifacts/phase3: initial_load, sql_fixtures, batch_recovery, dq_gate, airflow_run, dashboards và dashboard_reconciliation. Ops giữ cả lần chạy lỗi và lần hồi phục; các lỗi thử nghiệm không bị xóa để làm đẹp báo cáo.

Đã kiểm tra DAG manual/retry thật và quan sát các scheduled runs60 phút thành công trong thời gian kiểm thử kéo dài. Evidence tổng hợp ở artifacts/phase4/dag_run_summary.csv và scheduled_runs.csv. Full restatement facts/snapshots là chủ đích ở quy mô local; extract vẫn incremental. Bounded backfill giới hạn vùng snapshot, facts hiện hành được restate đầy đủ để giữ dependency và eligibility đúng.

Đã sửa lỗi quyền temp table, export artifacts của user Airflow và clock-skew JWT khi chạy thực tế. Airflow dùng leeway120 giây vì Docker/Windows lệch khoảng86 giây; không tắt xác thực. Job thành công đã commit không bị ghi lại FAILED chỉ do lỗi xuất artifact; task audit dựa vào ops và Airflow logs.

Trình duyệt không được cấp quyền mở localhost nên chưa chụp/render QA giao diện. Dashboard layout, saved questions, values và filters đã kiểm tra qua API; không có screenshot giả hoặc workaround để vượt từ chối quyền.

## Bàn giao Phase 4

Giữ9 KPI lịch sử hiện có; bổ sung Kafka producer/consumer, canonical raw events/delivery ledger, mart cửa sổ15 phút, DAG5 phút, RT01/RT02 và dashboard thứ tư. Không cộng events vào historical counts. Đo publish→dashboard-query qua ít nhất100 live events; chỉ công bố kết quả đo thực.
