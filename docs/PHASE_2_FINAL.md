# Phase 2 — Foundation và dữ liệu synthetic

**Hoàn tất ngày 23/10/2026.** Đã kiểm thử runtime trên Docker Desktop Linux engine, không còn bị chặn bởi lỗi Docker trước đó.

## Đã làm gì và để làm gì

| Công việc | Mục đích | Kết quả thực tế |
|---|---|---|
| Dựng Compose, PostgreSQL OLTP/DWH/metadata, Airflow API/scheduler/dag-processor, Kafka KRaft và Metabase | Có môi trường thống nhất cho batch và streaming | Các DB/Airflow/Kafka healthy; Metabase chạy và truy vấn được warehouse |
| Áp dụng DDL, migration checksum, roles và quyền | Có 10 bảng nghiệp vụ, schema raw/stg/dw/mart/ops; tránh sửa migration đã chạy | DDL chạy thành công; reader không ghi được nguồn; reporter đọc mart, không đọc raw |
| Historical generator 12 tháng, seed cố định, small/baseline | Cung cấp dữ liệu đúng quan hệ và có thể tái lập | 10.000 bệnh nhân, 10 khoa, 100 bác sĩ, 240 giường, 50.000 lịch hẹn, 50.000 OP, 4.000 IP, 32.000 lab orders |
| Mô phỏng ngoại lệ sạch và kiểm tra nguồn | Bao phủ walk-in/no-show/hủy/đổi lịch, OP→IP, chuyển giường, maintenance, lab hủy/trễ | Source checks không có lỗi; negative data chỉ ở tests |
| Seed bất biến, sync sequence và thử raw/stg | Sẵn sàng đầu vào cho warehouse; chạy lại không nhân entity | Đối soát đủ 151.433 rows trên 10 bảng; rerun inserted=0 cho mọi bảng; raw audit có batch riêng, staging giữ một dòng/PK |
| Restart DB và chạy smoke lại | Xác nhận volume lưu bền dữ liệu | Hai database restart thành công, counts và source integrity giữ nguyên |
| Cấu hình Metabase application DB riêng và warehouse reporter | Giữ metadata BI và dùng kết nối chỉ đọc | Đăng nhập/setup thành công; saved datasource Hospital Warehouse; query mart.foundation_health trả ready=1 |
| Khóa image digest, ghi cấu hình/resource evidence, khởi tạo Git local | Dễ dựng lại và theo dõi thay đổi | .env.example pin digest; .env secrets riêng; Git local đã khởi tạo, chưa publish GitHub vì chưa xác định remote |

## Kết quả kiểm thử

- 34/34 kiểm tra thiết kế Phase1 và 6/6 unit tests generator đạt.
- 5/5 PostgreSQL negative tests đạt: FK mồ côi, status sai, waiting âm, bed overlap, thay target TAT đã đóng băng đều bị từ chối.
- Nguồn và staging có cùng bộ business keys/counts; walk-in, OP→IP và giường có history đúng interval [start,end).
- Historical window: 22/10/2025 đến trước 22/10/2026, giờ Việt Nam. Snapshot hash `bc49c9638c25cab0c49f7dfd974ead4d1debd25463e7938c4b35169cb4799dcf`.
- Triggerer không chạy vì chưa có deferrable tasks; LocalExecutor giới hạn parallelism=2. UI/database ports chỉ bind loopback.

Evidence máy đọc ở artifacts/phase2: idempotent_rerun.json, source_database_tests.json, metabase_health.json, containers.jsonl, resources.jsonl; image identities ở infrastructure/images.lock.json. Không tạo báo cáo test Markdown riêng.

## Bàn giao sang Phase 3

Đã qua gate nền tảng: clean schema, đủ ba domain/12 tháng, source integrity, raw/stg smoke và dịch vụ điều phối/BI sẵn sàng. Phase3 sẽ triển khai incremental 60 phút, dimensional warehouse, 9 KPI và 3 dashboard. Chưa coi các KPI/DAG nghiệp vụ hoặc streaming đã hoàn thành trong Phase2. Hướng dẫn dựng lại và lệnh chạy nằm ở README.
