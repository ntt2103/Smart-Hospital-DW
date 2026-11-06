# Phase 1 — Thiết kế và chuẩn bị triển khai

Ngày rà soát: 23/10/2026, giờ Việt Nam. Phạm vi: thiết kế và kiểm chứng logic; chưa nghiệm thu hệ thống chạy thực tế.

## Công việc đã làm

- Khóa thiết kế 10 bảng nguồn, hành trình walk-in/đặt lịch → khám → nhập viện → chuyển/trả giường → xét nghiệm. PK BIGINT, timestamps UTC; báo cáo theo Asia/Ho_Chi_Minh.
- Hoàn thành [ERD nguồn](diagrams/oltp_erd.mmd), [star schema](diagrams/dw_star_schema.mmd) và [DDL nguồn](../sql/design/source_schema_draft.sql). DDL là nguồn tra cứu cột, kiểu, nullable, khóa và constraint; không cần thêm tài liệu cho từng phần code.
- Thiết kế 6 dimensions và 7 facts trong [warehouse DDL](../sql/design/warehouse_schema_draft.sql), [11 hàm KPI](../sql/design/kpi_queries.sql), raw và ops phục vụ audit/checkpoint/quarantine.
- Chốt incremental, SCD, snapshot giường, replay/dedup, event-time window, producer recovery và quyền truy cập.
- Tạo [JSON Schema](../src/streaming/contracts/event.schema.json), 15 sample events phủ 13 event types và [28 fixtures có đáp án](../tests/fixtures/phase1_cases.json).
- Thiết kế 4 dashboard và cách gắn đúng kỳ báo cáo cho từng KPI.
- Rà checklist chuẩn bị Phase 1. Bản cũ có 32 kiểm tra đạt, 2 lỗi artifact/link; file review/signoff bị thiếu. Tài liệu này thay thế bộ tài liệu chia theo từng phần, ghi đầy đủ trạng thái review và các giới hạn còn lại. Validator được sửa để kiểm tra cấu trúc theo phase và không phụ thuộc các file đã xóa.

## Kết quả và quy tắc đã chốt

### Nguồn và hành trình

10 bảng: patients, departments, doctors, appointments, encounters, beds, bed_status_history, bed_assignments, lab_test_types, lab_orders. Mã bệnh nhân/bác sĩ/xét nghiệm chỉ synthetic; không dùng dữ liệu bệnh nhân thật.

Appointment có tối đa một OP encounter cùng bệnh nhân/khoa. Walk-in không cần appointment. ATTENDED phải có check-in; NO_SHOW chỉ phân loại sau giờ hẹn 24 giờ và khi chưa có check-in hợp lệ. Hủy, đổi lịch và lịch chưa phân loại không tham gia mẫu số no-show. Đổi lịch giữ ID cũ RESCHEDULED và tạo successor cùng bệnh nhân, không nhánh/vòng. Writer khóa appointment khi check-in/classify để tránh race.

OP chuyển IP tạo encounter mới cùng bệnh nhân, parent là OP đã bắt đầu khám; admission không trước consultation. Một OP tối đa một IP con. IP có thể nhập trực tiếp. Không đổi encounter_type của lượt đã tồn tại. Trạng thái OP REGISTERED → CHECKED_IN → IN_CONSULTATION → COMPLETED; IP ADMITTED → DISCHARGED. Hủy chỉ ở giai đoạn được phép trong source constraints; sửa sai hồi tố phải audit.

Khoảng giường/status dùng [start,end); null end là vô hạn. Không có overlap theo bed hoặc theo IP encounter. Status history liên tục từ commissioned_at, có một khoảng mở cuối. Assignment nằm trọn admission/discharge và được ACTIVE bao phủ toàn bộ. Maintenance không được cắt ngang assignment. Writer khóa encounter trước rồi các bed theo ID tăng dần; discharge đóng assignment trong cùng transaction. Snapshot occupancy tại đúng 00:00 giờ Việt Nam, theo khoa vật lý của giường.

Lab ORDERED → COLLECTED → PROCESSING → COMPLETED, hoặc hủy trước completed. Order sau lúc khám/nhập viện, cùng khoa encounter; kết quả có thể sau discharge. Target TAT copy từ catalog tại order và bất biến; không cập nhật lại từ catalog lúc tính KPI. FK/CHECK/EXCLUDE trong DDL chưa thay thế kiểm tra liên bảng, transition giữa phiên bản và full ACTIVE coverage; writer và DQ phải triển khai các kiểm tra đó.

### Warehouse và KPI

| Fact | Grain | Kết quả phục vụ |
|---|---|---|
| fact_appointment | appointment_id | OP03 |
| fact_outpatient_encounter | OP encounter_id | OP01/OP02 |
| fact_inpatient_encounter | IP encounter_id | IP01/IP03 |
| fact_bed_assignment | bed_assignment_id | Truy vết và transfer |
| fact_bed_snapshot_daily | bed × ngày local, lúc 00:00 | IP02 |
| fact_lab_order | lab_order_id | LAB01/LAB02/LAB03 |
| fact_operational_event | canonical event_id | RT01/RT02 |

Dimensions: date, patient, department, doctor, bed, lab_test_type; SK độc lập nguồn, unknown SK0. Doctor SCD2 chỉ phản ánh thay đổi quan sát được từ bootstrap/extract, không dựng lịch sử trước khi có bằng chứng. Fact giữ doctor business ID; lookup theo mốc nghiệp vụ, ngoài khoảng coverage → SK0. Các dimension còn lại mặc định SCD1. Khoa fact lấy từ chính row nghiệp vụ, không suy ngược từ khoa hiện tại của doctor.

| KPI | Công thức | Vai trò ngày |
|---|---|---|
| OP01 | distinct OP đã bắt đầu khám | consultation start |
| OP02 | tổng phút chờ hợp lệ / số mẫu chờ hợp lệ | consultation start |
| OP03 | 100 × NO_SHOW / (ATTENDED + NO_SHOW) đủ điều kiện | scheduled |
| IP01 | distinct IP có admission hợp lệ | admission |
| IP02 | 100 × occupied ACTIVE / ACTIVE capacity cùng snapshot | midnight snapshot |
| IP03 | tổng số ngày thực đã nằm viện / số discharge hợp lệ | discharge |
| LAB01 | distinct completed lab order | result completion |
| LAB02 | tổng phút order→result hợp lệ / số mẫu TAT hợp lệ | result completion |
| LAB03 | 100 × on-time orders / số mẫu có target hợp lệ | result completion |
| RT01 | distinct encounter có checked_in trong 15 phút event-time | cùng as_of |
| RT02 | distinct lab order có result_completed trong 15 phút event-time | cùng as_of |

Mẫu số 0 trả NULL/N/A. Tổng hợp tỷ lệ/trung bình bằng thành phần numerator/denominator, không AVG các tỷ lệ khoa. Cờ eligibility tách theo measure: lỗi waiting không tự xóa lượt khám, lỗi TAT không tự xóa throughput. Occupancy thiếu coverage hoặc snapshot invalid phải N/A và cảnh báo; không âm thầm dùng trạng thái giường hiện tại hoặc LIMIT 1 để che overlap.

### Batch, staging và vận hành

Luồng OLTP → raw.source_rows → staging typed/latest → dimensions → facts → daily mart views. Raw giữ batch UUID, source PK, source_updated_at, extracted_at và nguyên payload; business keys ở facts unique. Watermark (updated_at,PK), upper bound cố định, lookback 120 phút. Snapshot nguồn REPEATABLE READ; technical source timestamps tăng khi sửa hồi tố. Bound giao dịch nguồn 60 giây; periodic full reconciliation bắt trường hợp ngoài lookback. Chỉ advance watermark cùng commit thành công của merge/DQ. Retry/rerun/backfill không tạo entity mới; sai nghiêm trọng chặn downstream.

Batch Airflow mỗi 60 phút, retries/timeout và manual backfill rõ ràng. Fixed business cutoff source_now − 120 giây được ghi trong input_bounds; không trộn với cursor kỹ thuật. Điều kiện phụ thuộc thời gian phải được restate dù updated_at không đổi. Thay đổi encounter cần restate context của lab/assignment; correction bed status/assignment rebuild hợp khoảng cũ và mới, kể cả xóa snapshot không còn commissioned. Staging cùng timestamp nhưng khác payload phải báo conflict.

Ops: pipeline_runs, watermarks, dq_results, quarantined_records, reconciliation_results, mart_refreshes. Tám nhóm DQ: khóa, FK, status/transition, timeline, bed overlap, encounter overlap, history/capacity, reconciliation/freshness. Lỗi phải có severity/action và lineage; không đổi duration lỗi thành 0.

### Streaming và khôi phục

Ba topics: appointments.events (key appointment_id), encounters.events (key encounter_id), laboratory.events (key lab_order_id). Không bảo đảm thứ tự giữa keys/topics. Envelope gồm event_id UUID, event_type, entity_id, schema_version 1.0.0, event_time, published_at, source_system hospital_simulator và payload; consumer bổ sung topic/partition/offset/ingested_at, không sửa producer body.

Single-writer producer phải persist PREPARED command trong SQLite journal trên volume trước source mutation: command ID, fixed IDs/created_at, before/after image CAS, event UUID/payload. Commit OLTP rồi DB_APPLIED, publish retry sau đó. Recovery so exact before/after image (không so updated_at do trigger sinh); khác cả hai là conflict phải chặn. Persist published_at và event bytes trước lần gửi đầu; ACK rồi mới mark PUBLISHED. Crash ACK→mark thì gửi lại cùng UUID/body. Không tuyên bố atomic PostgreSQL + Kafka.

raw.events lưu canonical body bất biến theo event_id. raw.event_deliveries lưu từng vị trí Kafka, cả duplicate/malformed; cùng UUID khác body quarantine conflict. Commit consumer offset sau transaction raw/delivery/quarantine bền vững. Missing entity/dimension lookup giữ pending và retry, không gán unknown rồi sửa canonical event. RT eligibility kiểm topic/type/key/FK/khoa/timestamp, không đếm số message.

Near-RT Airflow mỗi 5 phút, cửa sổ (as_of−15 phút,as_of], có ingested_at≤as_of. Late event được tính lần sau nếu còn trong window; quá cũ vẫn lưu audit. Recompute toàn cửa sổ, distinct entity trước group; refresh khi idle về 0. Toàn bộ khoa publish atomic cùng as_of; dashboard đọc một snapshot đã hoàn tất, không cộng nhiều cửa sổ. Batch facts và streaming counts độc lập theo nguồn/grain, không cộng double-count.

### Dashboard và hạ tầng

D01 Executive: OP01/IP01/IP02/LAB01; D02 Outpatient: OP01–03; D03 Inpatient & Bed: IP01–03; D04 Laboratory & Real-Time: LAB01–03/RT01–02. Filter ngày/khoa phải đúng date role; occupancy chọn một snapshot date; RT chọn một as_of và không theo date range batch. Hiển thị batch completion, DQ, as_of/window/freshness. Reporter chỉ đọc mart/safe views. p95 publish→dashboard ≤10 phút là mục tiêu chưa đo; nghiệm thu cần ≥100 live events, clock thật, p50/p95/timeouts và điều kiện đo.

Topology dự kiến PostgreSQL OLTP/DWH riêng, metadata server có hai database Airflow/Metabase riêng; Airflow3 API/scheduler/dag-processor + LocalExecutor; Kafka single-broker KRaft; Metabase. Loopback host ports 55432/55433/18080/13000/19092; network nội bộ dùng service names. Persistent volumes cho DB, Kafka, journal và logs; mount code readonly. Cấu hình/secrets qua .env; không commit. Image candidates nằm trong [manifest](../infrastructure/versions.design.json); phải resolve digest/build/smoke thực tế ở Phase 2. Máy 4CPU/16GB, Docker 8–10GB chỉ là ước lượng, chưa benchmark.

## Kết quả kiểm tra và bàn giao

Chạy `node scripts/validate_design.mjs`: kết quả sau rà soát **34 đạt, 0 lỗi**. Evidence máy đọc tại [artifacts/phase1/validation_report.json](../artifacts/phase1/validation_report.json). Phạm vi: fixture oracle, inventory SQL/artifact, JSON samples và links; không chứng minh PostgreSQL/Kafka/Airflow/Metabase đã chạy.

Tài liệu này là kết quả review kỹ thuật, không ký thay người phụ trách. Các tên người/deadline/tài nguyên thực tế chưa xác nhận không chặn triển khai local. Checklist chuẩn bị cũ được thay bằng bản tổng kết này; runtime DDL, full-scale data integrity, DAG schedules, BI, failure injection và latency được xác nhận ở phase triển khai tương ứng trước khi chuyển bước.

Kế hoạch tiếp theo: [roadmap Phase 2–4](PHASE_2_3_4_EXECUTION_ROADMAP.md). Mỗi phase đã hoàn tất chỉ giữ một file tổng kết; evidence JSON/log/test code để ngoài docs, không tạo file Markdown kiểm thử riêng.
