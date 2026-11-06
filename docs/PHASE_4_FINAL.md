# Phase 4 — Kafka và near real-time BI

**Hoàn tất ngày23/10/2026 về chức năng, integration và phép đo API-readiness.** Có ba topics, producer/consumer thực, mart event-time, hai KPI RT và dashboard thứ tư; giữ nguyên9 KPI lịch sử. Giới hạn clock/UI được ghi rõ bên dưới.

## Đã làm gì, để làm gì và kết quả

| Công việc | Mục đích | Kết quả đã kiểm tra |
|---|---|---|
| Tạo appointments.events, encounters.events, laboratory.events; schema chuẩn và13 types | Có hợp đồng event/version/key thống nhất | Ba topics, mỗi topic3 partitions; đủ13 event types đã qua producer→Kafka→raw |
| Live synthetic simulator và single-writer SQLite journal trên volume | Ghi nguồn trước publish, khôi phục khi crash hoặc broker lỗi | PREPARED→DB_APPLIED→PUBLISHED; before/after image kiểm tra nguồn, fixed UUID/body/published_at; crash sau source commit và broker không kết nối đều hồi phục, không nhân entity |
| Consumer liên tục, raw.events canonical và event_deliveries | Không mất dấu message và không đếm duplicate | Same UUID cùng body dedup; body conflict/invalid bytes vào quarantine; commit offset sau transaction; restart bắt kịp và replay1076 vị trí không làm tăng canonical/deliveries |
| Source semantic validation và reconciliation missing signals | Ngăn payload sai nguồn và gửi lại signal bị thiếu | Validate topic/key/schema/version/context/milestone; source→raw reconciliation resend cùng bytes; clock-future failures có audit, không ghi đè canonical |
| Operational event fact và atomic near-RT snapshot | RT theo thời điểm nghiệp vụ, tách batch | RT01 distinct encounter checked_in, RT02 distinct lab order result_completed; (as_of−15m,as_of], ingested_at≤as_of; cùng as_of cho mọi khoa |
| Airflow hospital_near_rt mỗi5 phút | Cập nhật mart ngay cả khi không có event mới | DAG thực đã success; boundary/future/dedup/idle-expiry4 SQL assertions đạt; consumer vẫn ingest khi DAG tạm pause, unpause/trigger bắt kịp |
| Dashboard Laboratory & Real-Time | Hiển thị LAB01–03 và RT01/RT02 với freshness | Dashboard5, saved questions chạy;5 scalar cards đối chiếu Metabase/SQL đạt; có as_of/window_start/window_end/last_updated |
| Đo live API-readiness và regression | Kiểm chứng độ trễ, giữ đúng batch | 100/100 events thấy qua Metabase, timeout0;9 KPI lịch sử tiếp tục đối soát đúng OLTP |

## Kết quả latency thực tế

- Sample size: **100 live KPI events**, gồm check-in và lab result; không dùng clock tăng tốc.
- **p50=222.68 giây; p95=222.68 giây**, so với mục tiêu600 giây: **đạt theo thời gian elapsed đo được**.
- Refresh bởi DAG cron5 phút, không trigger refresh thủ công trong phép đo; observer gọi Metabase mỗi5 giây.
- Mốc đầu: query clock PostgreSQL gán published_at bất biến trước publish. Mốc cuối: lần Metabase API đầu tiên trả event trong snapshot mới nhất. Cùng process ghi monotonic anchor nên phép đo không bị duration âm khi wall clock nhảy.
- Phép trừ hai UTC timestamps đơn thuần có reliable=False; sai khác lớn nhất với elapsed=89.77 giây. CSV/JSON giữ cả hai loại số đo. Đây là **publish→Metabase-query readiness**, chưa đo browser rendering/caching/frame hiển thị.

## Vận hành và giới hạn

[Dashboard thứ tư](http://localhost:13000/dashboard/5#refresh=60). Ba dashboard Phase3 tiếp tục dùng được. Date range lịch sử mặc định không áp lên RT; RT đọc một as_of đã publish và dùng filter khoa. Khi idle, window tự hết hạn về0 khi DAG refresh, không giữ count cũ.

Clock Windows–WSL vẫn có thể lệch/nhảy khoảng90 giây; một lần đồng bộ bằng date không giữ được lâu. Không sửa Windows time/timezone. Airflow giữ JWT leeway120 giây, vẫn xác thực token. Source DB là clock authority cho business/published_at/future validation; refresh dùng clock DWH cùng ingested_at. Producer trì hoãn publish khi source clock lùi và giữ journal để recover, không gửi timestamp sai để ép test pass.

UI browser chưa được cấp quyền nên không có screenshot/render QA. Đã kiểm tra saved dashboard layout, query values và filters qua API; không suy đó thành pixel/render evidence. Kafka là single broker local, không HA. Journal/single writer là giải pháp local, không tuyên bố atomic PostgreSQL+Kafka hay exactly-once toàn hệ thống.

## Evidence và bàn giao

Code nằm ở src/streaming/, src/batch_pipeline/jobs/near_rt.py, src/batch_pipeline/dags/hospital_near_rt.py và src/analytics/. Evidence JSON/CSV ở artifacts/phase4: streaming_failure_tests, window_sql_tests, pause_recovery, airflow_run, dashboard_reconciliation, historical_regression, consumer_offsets, latency_measurements, clock_status và resources. Không tạo Markdown test report riêng.

Các lỗi/failure injection/quarantine được giữ để truy vết. Producer phát theo lệnh, không chạy vòng lặp nền sau demo; consumer, Kafka, Airflow, Metabase và DB giữ chạy để người dùng xem kết quả. Phase5 có thể tập trung QA dài hạn, UI review khi có quyền, xử lý clock WSL bền vững và demo; không thêm nghiệp vụ ngoài baseline.

## Mở rộng demo đặt lịch qua form local — 23/10/2026

Theo yêu cầu bổ sung, đã có form tại **http://localhost:18081**. 151.433 records ban đầu là seed lịch sử, không phải giới hạn dữ liệu; source có thể tăng bằng producer hoặc form. Demo dùng bệnh nhân synthetic có sẵn, không thêm tên/điện thoại hay bảng nghiệp vụ mới.

| Đã làm gì | Để làm gì | Kết quả đạt được |
|---|---|---|
| Form chọn mã bệnh nhân, khoa, bác sĩ và giờ khám Việt Nam | Cho người demo nhập nghiệp vụ trực tiếp thay vì chạy script | Gửi form tạo `appointments` trạng thái `SCHEDULED` ngay trong OLTP; chọn ngày tương lai tối đa365 ngày |
| API kiểm tra bệnh nhân/danh mục hoạt động, bác sĩ thuộc khoa và thời gian | Chặn dữ liệu không hợp lệ trước khi ghi | Mã sai, khoa/bác sĩ không khớp, giờ quá khứ, giờ không có timezone, ID kiểu boolean đều bị từ chối |
| UUID yêu cầu và journal riêng trên volume booking-journal | Chống tạo trùng khi retry, mất kết nối hoặc reload | Bốn lần gửi đồng thời cùng request chỉ có một appointment; đổi nội dung cùng UUID trả409; +07:00/Z tương đương có cùng kết quả |
| Ghi source trước, worker gửi event created theo hợp đồng hiện có | Đưa dữ liệu nhập tay vào Kafka và luồng đang chạy | Hai lịch hẹn thử50002/50003 đã vào OLTP→Kafka→raw→operational fact→DW fact appointment |
| Retry journal khi crash/publish lỗi và cách ly command conflict | Giữ yêu cầu đã nhận và tiếp tục xử lý yêu cầu khác | Crash sau SQL commit không tạo lại appointment; broker giả lập mất kết nối giữ nguyên bytes/UUID rồi gửi lại thành công; unit test chứng minh conflict không chặn command khác |
| Mở rộng dim_date theo ngày hẹn tương lai | Tránh lỗi FK khi đặt lịch xa hơn ngày mai | Lịch khám22/11/2026 được nạp vào date dimension/fact đúng; không tạo thêm bed snapshot tương lai |
| Bảng tiến độ thực trên form | Phân biệt ghi nguồn với độ trễ phân tích | Hiển thị source, broker ACK, raw, operational fact và DW; poll4 giây tối đa5 phút rồi kiểm tra thủ công |
| Docker service booking, cổng chỉ127.0.0.1:18081 | Chạy demo trên máy cùng stack có sẵn | Container healthy; restart giữ journal và truy vết được lịch hẹn cũ; Host/Origin khác và POST không phảiJSON bị chặn |

**Kết quả kiểm chứng:** 14 Python unit tests đạt (8 cho src/booking/recovery,6 generator); kiểm thử JavaScript bằng mock DOM đạt cho catalogs, timezone, retry, reload và tạo yêu cầu mới;34 checks thiết kế cũ vẫn đạt. Integration HTTP/SQL/Kafka và đối soát lại9 KPI lịch sử đạt, consumer lag0. Không tạo encounter/lab khi đặt lịch; `SCHEDULED` không vào KPI lượt khám/check-in/xét nghiệm hay mẫu số no-show.

Nguồn được lưu ngay sau phản hồi xác nhận; consumer chạy liên tục, operational fact cập nhật theo DAG5 phút và DW theo batch mỗi giờ. Trong acceptance đã chạy batch/near-RT thủ công để kiểm chứng, không dùng kết quả này thay phép đo scheduled latency ở trên. Snapshot kiểm tra đầu chưa chứa event thứ hai vì ingested_at04:02:49UTC nằm sau as_of04:02:37UTC; snapshot sau04:03:18UTC bắt kịp đúng. Không sửa timestamp hoặc clock để ép qua kiểm tra.

Evidence bổ sung nằm trong `artifacts/phase4/booking_demo.json` và `booking_validation.json`; code/tests ở src/booking/ và tests/. Giao diện được kiểm tra HTTP và logic JavaScript, chưa có browser render QA. Không tạo thêm file docs theo module hoặc test.

Form hiện chỉ đặt lịch cho bệnh nhân mẫu có sẵn; chưa thêm đăng ký bệnh nhân, kiểm tra lịch làm việc/sức chứa bác sĩ, đổi/hủy lịch hay Google Forms. Nếu một command PREPARED gặp source đã thay đổi không khớp after-image, command được giữ để đối soát thủ công; các command khác vẫn tiếp tục publish. Không đoán lại lịch sử hoặc tự ghi đè nguồn. Cách mở, thao tác demo, lệnh bật service và giới hạn có trong README.
