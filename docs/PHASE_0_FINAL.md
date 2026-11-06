# SMART HOSPITAL DATA WAREHOUSE — PHASE 0 FINAL

> **Project:** Smart Hospital Data Warehouse & Near Real-Time Analytics  
> **Target release:** V2.0 — Academic/Local Demo  
> **Phase:** 0 — Project Initiation & Requirement Definition  
> **Document version:** 1.0 (Approved Baseline)  
> **Baseline date:** 2026-10-22  
> **Status:** **PHASE 0 CLOSED — READY FOR PHASE 1**  
> **Next document:** [`PHASE_1_FINAL.md`](./PHASE_1_FINAL.md)

## 1. Tóm tắt quyết định (Decision Register)

Người phụ trách đồ án đã xác nhận đề mục 01–02 và đồng ý với bản đề xuất 03–12 để khóa Phase 0. Đây là **baseline thiết kế**; thay đổi về sau cần được ghi rõ lý do và tác động.

| ID | Đề mục | Quyết định đã chốt |
|---|---|---|
| P0-01 | Vision & Objectives | **Unified Analytics Platform**, 5 mục tiêu O1–O5 |
| P0-02 | Processes & Boundaries | **Integrated Patient Journey**; đầy đủ 3 domain ngoại trú, nội trú, xét nghiệm; có walk-in, OP→IP, chuyển giường, ngoại lệ |
| P0-03 | KPI Catalog | **11 KPI bắt buộc**, 4 KPI mở rộng không bắt buộc |
| P0-04 | Sources & Entities | **10 bảng OLTP lõi**, **3 Kafka topics** |
| P0-05 | Synthetic Data | Dữ liệu giả lập **12 tháng**; historical generator + live event generator |
| P0-06 | Architecture | **Hybrid Batch + Streaming**, các lớp `raw` → `stg` → `dw` → `mart`, `ops` |
| P0-07 | Technology Stack | Python, PostgreSQL, Airflow, Kafka, SQL, Metabase, Docker Compose, Git/GitHub |
| P0-08 | Processing | Batch incremental 60 phút; stream consumer liên tục; near-RT mart 5 phút; mục tiêu p95 ≤ 10 phút |
| P0-09 | Quality & Governance | Tối thiểu **8 nhóm kiểm thử DQ**, idempotency, quarantine, đối soát, không dùng dữ liệu bệnh nhân thật |
| P0-10 | BI Scope | **4 dashboard** chính, bộ lọc theo thời gian/khoa và giám sát chất lượng dữ liệu |
| P0-11 | Plan | **6 phase (0–5)**; khung dự trù 6–8 tuần, điều chỉnh theo nhân lực thực tế |
| P0-12 | Acceptance | Demo end-to-end, 11 KPI, Kafka/Airflow, xử lý trùng/đến muộn, DQ, tài liệu và kiểm thử độ trễ |

## 2. Project Charter

### 2.1. Problem Statement

Các nghiệp vụ khám ngoại trú, điều trị nội trú và xét nghiệm của bệnh viện giả lập phát sinh dữ liệu ở những tiến trình khác nhau. Nếu chỉ truy vấn trên từng hệ thống giao dịch, việc tính KPI lịch sử, đo thời gian chờ, theo dõi công suất giường và giám sát hoạt động gần thời gian thực thiếu mô hình và định nghĩa nhất quán.

Dự án xây dựng **Data Warehouse dùng chung**, có khả năng tiếp nhận dữ liệu giao dịch theo batch và sự kiện nghiệp vụ qua Kafka, phục vụ các dashboard vận hành và cho phép đối soát, truy vết, tái xử lý.

### 2.2. Project Vision

> Xây dựng một nền tảng dữ liệu tập trung cho bệnh viện thông minh, tích hợp dữ liệu ngoại trú, nội trú và xét nghiệm, hỗ trợ phân tích hiệu suất vận hành, sử dụng tài nguyên và giám sát các hoạt động quan trọng gần thời gian thực.

### 2.3. Business Objectives

| ID | Objective | Kết quả mong đợi |
|---|---|---|
| O1 | **Unified Data** | Định danh chung theo bệnh nhân, encounter, khoa và thời gian; phân tích xuyên domain |
| O2 | **Operational Efficiency** | Theo dõi lượt khám, thời gian chờ, no-show và hiệu suất xét nghiệm |
| O3 | **Resource Optimization** | Theo dõi nhập viện, thời gian nằm viện và tỷ lệ sử dụng giường |
| O4 | **Near Real-Time Visibility** | Theo dõi check-in và xét nghiệm hoàn thành trên cửa sổ 15 phút |
| O5 | **Data Reliability** | Kiểm soát chất lượng, dedup, lịch sử thay đổi, đối soát, rerun an toàn |

### 2.4. Stakeholders giả lập

| Vai trò | Nhu cầu |
|---|---|
| Hospital/Operations Management | Dashboard tổng quan hoạt động |
| Outpatient Manager | Lượt khám, chờ khám, no-show |
| Inpatient/Bed Coordinator | Admissions, LOS, occupancy |
| Laboratory Manager | Thông lượng xét nghiệm, TAT, hoàn thành đúng hạn |
| Data Engineering Team | Pipeline, event ingestion, freshness, chất lượng và tính đúng KPI |

### 2.5. Giới hạn sản phẩm

Dự án là **nền tảng phân tích dữ liệu** và một **hệ thống nguồn giả lập**. **Không** phát triển phần mềm bệnh viện production hay đưa ra quyết định lâm sàng tự động. Hệ thống chạy local, ưu tiên tính khả thi và khả năng trình bày trong đồ án môn Data Warehouse.

## 3. Business Processes — Integrated Patient Journey

### 3.1. Outpatient

1. Bệnh nhân **đặt lịch** hoặc **walk-in** không đặt lịch.
2. Check-in → bắt đầu khám → hoàn thành khám; một lượt khám có thể chỉ định nhiều xét nghiệm.
3. Lịch hẹn có thể `CANCELLED`, `RESCHEDULED`, `ATTENDED`, `NO_SHOW`.
4. Trường hợp chuyển nội trú: tạo **encounter INPATIENT mới**, liên kết encounter OUTPATIENT bằng `parent_encounter_id`.

### 3.2. Inpatient

1. Nhập viện trực tiếp hoặc chuyển từ encounter ngoại trú.
2. Cấp giường → điều trị → có thể chuyển giường → xuất viện → trả giường.
3. Quản lý **lịch sử cấp/trả giường theo khoảng thời gian**, không chỉ trạng thái mới nhất.
4. Một encounter nội trú chỉ được chiếm tối đa **một giường tại một thời điểm**; một giường không được gán chồng lấn cho nhiều encounter.

### 3.3. Laboratory

1. Yêu cầu xét nghiệm gắn với một encounter OP hoặc IP.
2. Order → sample collected → processing → result completed; hỗ trợ cancelled.
3. Theo dõi `ordered_at`, `collected_at`, `processing_started_at`, `result_completed_at` và loại xét nghiệm.
4. Chỉ phân tích vận hành/order metadata; không phân tích kết quả y khoa chi tiết.

### 3.4. Các trường hợp ngoại lệ bắt buộc

Walk-in, hủy/đổi lịch, no-show, OP→IP, chuyển giường, giường bảo trì, xét nghiệm hủy/trễ, event đến trùng, event đến muộn và bản ghi sai dữ liệu trong tập kiểm thử.

### 3.5. In Scope / Out of Scope

| In scope V2.0 | Out of scope V2.0 |
|---|---|
| Ngoại trú, nội trú, xét nghiệm với hành trình liên kết | Bệnh án y khoa chi tiết, chẩn đoán/phác đồ |
| Sinh dữ liệu OLTP và event synthetic | Dữ liệu bệnh nhân thật, HIS/EMR thật |
| Batch incremental + Kafka ingestion | Cam kết xử lý real-time sub-second |
| Dimensional warehouse, marts, KPI, BI | Bảo hiểm, thu phí, thuốc, phẫu thuật phức tạp |
| DQ, đối soát, xử lý lại, demo local | Spark, Flink, Kubernetes, HA/cloud production |

## 4. KPI Catalog — 11 KPI bắt buộc

### 4.1. Quy ước dùng chung

- **Timestamp lưu UTC**; kỳ báo cáo theo `Asia/Ho_Chi_Minh`.
- Đếm theo **distinct business key ở đúng grain**, không coi mỗi event/row raw là một lượt nghiệp vụ.
- Dữ liệu không hợp lệ (thiếu mốc, thời gian âm, trạng thái sai) được đánh dấu DQ; không mặc nhiên điền bằng 0 trong phép trung bình.
- Ngày ghi nhận có thể **khác nhau theo KPI**: ngày khám, ngày hẹn, ngày nhập, ngày xuất, ngày hoàn thành xét nghiệm hay ngày snapshot.
- Với tỷ lệ có mẫu số bằng 0, trả `NULL`/`N/A`, không trả 0% giả tạo.
- Các công thức dưới đây là **business definition cuối Phase 0**; Phase 1 ánh xạ xuống fact, dimension và SQL cụ thể.

| ID | Domain | KPI | Công thức/định nghĩa | Mốc ngày/kỳ báo cáo |
|---|---|---|---|---|
| **OP01** | Outpatient | Outpatient Visits | `COUNT(DISTINCT encounter_id)` với `encounter_type='OUTPATIENT'` và `consultation_started_at` hợp lệ | Ngày bắt đầu khám |
| **OP02** | Outpatient | Avg Waiting Time (min) | `AVG(consultation_started_at - checked_in_at)` theo phút; chỉ nhận cặp timestamp hợp lệ, không âm | Ngày bắt đầu khám |
| **OP03** | Outpatient | Appointment No-show Rate (%) | `NO_SHOW / (ATTENDED + NO_SHOW) * 100`; chỉ lịch đến hạn và đã phân loại, loại `CANCELLED` và `RESCHEDULED` | Ngày lịch hẹn |
| **IP01** | Inpatient | Total Admissions | `COUNT(DISTINCT encounter_id)` của encounter INPATIENT có `admitted_at` hợp lệ | Ngày nhập viện |
| **IP02** | Inpatient | Bed Occupancy Rate (%) | `số giường ACTIVE đang có assignment / tổng số giường ACTIVE * 100` tại thời điểm snapshot **00:00 giờ Việt Nam** | Ngày snapshot, theo khoa |
| **IP03** | Inpatient | Average Length of Stay (days) | `AVG((discharged_at - admitted_at) / 24 giờ)` cho đợt đã xuất viện, thời gian hợp lệ | Ngày xuất viện |
| **LAB01** | Laboratory | Completed Lab Orders | `COUNT(DISTINCT lab_order_id)` có trạng thái completed và `result_completed_at` hợp lệ | Ngày kết quả hoàn thành |
| **LAB02** | Laboratory | Average Lab TAT (min) | `AVG(result_completed_at - ordered_at)` phút, chỉ orders đã completed và có timeline hợp lệ | Ngày kết quả hoàn thành |
| **LAB03** | Laboratory | On-time Lab Result Rate (%) | `completed orders với TAT <= target_tat_minutes / all valid completed orders * 100` | Ngày kết quả hoàn thành |
| **RT01** | Streaming | Check-ins Last 15 Minutes | `COUNT(DISTINCT encounter_id)` của event `checked_in` hợp lệ với `event_time` thuộc `(as_of - 15 phút, as_of]` | Cửa sổ trượt event-time |
| **RT02** | Streaming | Lab Results Last 15 Minutes | `COUNT(DISTINCT lab_order_id)` của event `result_completed` hợp lệ cùng cửa sổ | Cửa sổ trượt event-time |

**Chính sách đo lường chi tiết:**

1. **No-show:** một lịch được kết luận sau giờ hẹn và hết khoảng chờ phân loại (mặc định mô phỏng: sau 24 giờ); `RESCHEDULED` giữ dấu vết, không tính mẫu số; lịch mới có ID riêng. Walk-in không thuộc mẫu số no-show.
2. **Occupancy:** khoảng cấp giường dạng **[assigned_at, released_at)**; giường `ACTIVE` tính vào capacity, giường `MAINTENANCE/INACTIVE` bị loại. Trạng thái giường phải tra **tại đúng timestamp snapshot**, không dùng trạng thái hiện tại. Bản ghi bed assignment chồng lấn là DQ error.
3. **LOS:** dùng đợt INPATIENT đã xuất viện; số ngày có thể là số thực, tính bằng thời lượng giờ/24, không làm tròn trước AVG.
4. **Lab TAT:** `ordered_at → result_completed_at`; chỉ số đúng hạn dựa trên `target_tat_minutes` của **loại xét nghiệm tại thời điểm tạo order**, nên giá trị target được đóng băng/tra theo hiệu lực lịch sử ở Phase 1. Đây là ngưỡng mô phỏng, không phải chuẩn y khoa thực tế.
5. **Streaming:** `event_time` dùng để chọn cửa sổ; `published_at`/`ingested_at` dùng đo latency; trùng `event_id` phải bị dedupe. Near-RT mart có thời điểm tính `as_of` rõ ràng. Sự kiện đến muộn được nhận lại và tính khi refresh sau nếu nằm trong cửa sổ, đồng thời có đường đối soát lịch sử.

### 4.2. KPI mở rộng (không phải điều kiện hoàn thành V2.0)

| ID | KPI | Ghi chú |
|---|---|---|
| X01 | Outpatient → Inpatient Conversion Rate | Dựa trên `parent_encounter_id` |
| X02 | Bed Transfer Count | Dựa trên chuỗi `bed_assignments` |
| X03 | Near Real-Time Bed Occupancy | Cần cập nhật trạng thái tại event time |
| X04 | Pending Lab Order Backlog | Dựa trên order còn mở tại thời điểm đo |

## 5. Data Sources & Data Contract — baseline

### 5.1. 10 bảng OLTP lõi

| Domain | Bảng | Vai trò |
|---|---|---|
| Shared | `patients` | Bệnh nhân synthetic |
| Shared | `departments` | Khoa/phòng |
| Shared | `doctors` | Bác sĩ và khoa công tác |
| Outpatient | `appointments` | Lịch hẹn, trạng thái, dời lịch |
| OP + IP | `encounters` | Lượt khám ngoại trú/đợt điều trị nội trú, các timestamp nghiệp vụ |
| Inpatient | `beds` | Danh mục giường |
| Inpatient | `bed_status_history` | Lịch sử ACTIVE/MAINTENANCE/INACTIVE của giường |
| Inpatient | `bed_assignments` | Lịch sử gán, chuyển, trả giường |
| Laboratory | `lab_test_types` | Danh mục xét nghiệm và target TAT |
| Laboratory | `lab_orders` | Yêu cầu xét nghiệm, trạng thái và timestamps |

Các bảng OLTP thuộc **đơn vị giao dịch**, không thay thế bằng fact/dimension. Thiết kế cột, PK/FK, cardinality, status dictionary, unique constraints và ERD là nội dung Phase 1.

### 5.2. Kafka topics

| Topic | Event types tối thiểu |
|---|---|
| `appointments.events` | `created`, `status_changed` |
| `encounters.events` | `checked_in`, `consultation_started`, `admitted`, `bed_assigned`, `bed_released`, `discharged` |
| `laboratory.events` | `order_created`, `sample_collected`, `processing_started`, `result_completed`, `cancelled` |

Envelope tối thiểu: `event_id`, `event_type`, `entity_id`, `schema_version`, `event_time`, `published_at`, `payload`. Phase 1 bổ sung `source_system`, key partition, JSON schema, các trường bắt buộc và quy tắc tương thích.

**Nguồn sự thật:** PostgreSQL OLTP là nguồn dữ liệu giao dịch đối soát; Kafka cung cấp signal sự kiện gần thời gian thực. Batch và stream **không được cộng gộp naïve** để đếm hai lần cùng một encounter/order. Luồng phát event cần có retry và cơ chế đối soát khi publish lỗi; không giả định transaction PostgreSQL + Kafka là atomic.

## 6. Synthetic Data Strategy

- **Historical generator:** sinh 12 tháng lịch sử bằng Python với random seed cố định, quan hệ thực thể hợp lệ và thời gian sự kiện hợp lý.
- **Live generator:** ghi thay đổi nghiệp vụ vào OLTP giả lập rồi publish event lên Kafka, tạo load demo theo giờ hoặc tốc độ cấu hình được.
- **Test mode:** sinh dữ liệu lỗi/trùng/đến muộn riêng để test DQ và idempotency; không làm hỏng bộ dữ liệu sạch mặc định.

| Dữ liệu | Quy mô mục tiêu |
|---|---:|
| Patients | ~10.000 |
| Departments | 10–12 |
| Doctors | 80–120 |
| Beds | 200–300 |
| Appointments | ~50.000 |
| Outpatient Encounters | 45.000–60.000 |
| Inpatient Encounters | 3.000–6.000 |
| Lab Orders | 25.000–40.000 |

Đây là **target quy mô**, không phải số lượng dữ liệu đã sinh. Ưu tiên đúng business rules và KPI hơn số lượng bản ghi.

## 7. High-Level Architecture & Technology

```text
                         +---------------------+
                         | Python Data Simulator|
                         +----------+----------+
                                    |
                       +------------+------------+
                       |                         |
                       v                         v
               PostgreSQL OLTP              Kafka Topics
                       |                         |
                       | incremental ETL         | Python Consumer
                       v                         v
                  Airflow DAGs               DWH raw.events
                       |                         |
                       v                         | Airflow micro-batch
                 DWH raw -> stg                   v
                       |                 near-real-time marts
                       v                         |
                    DWH dw                       |
                       |                         |
                       +------------+------------+
                                    v
                                DWH mart
                                    |
                                    v
                                 Metabase
```

- `raw`: dữ liệu copy từ nguồn, payload events và ingestion metadata.
- `stg`: chuẩn hóa, lọc/dedupe, business validity và trạng thái hợp lệ.
- `dw`: mô hình dimensional theo fact/dimension và lịch sử được xác định.
- `mart`: tập hợp/tổng hợp hỗ trợ KPI, gồm near-real-time.
- `ops`: logging, watermark, DQ results, quarantine, reconciliation.

**Stack chốt:** Python · PostgreSQL (OLTP + DWH; Airflow metadata tách logic) · Apache Airflow · Apache Kafka single-broker KRaft · SQL + Python transform · Metabase · Docker Compose · Git/GitHub.

**Không yêu cầu V2.0:** Spark, Flink, Kubernetes, Debezium, dbt, hạ tầng cloud phân tán.

## 8. Processing & Non-functional Requirements

| Hạng mục | Baseline |
|---|---|
| Incremental batch | Mỗi **60 phút** (Airflow) |
| Kafka consumer | Chạy liên tục; xử lý **at-least-once** |
| Near-RT mart | Refresh mỗi **5 phút** |
| Dashboard | Mục tiêu refresh tự động 1 phút khi demo; manual khi cần |
| Streaming dedupe | `event_id` unique/idempotent; xử lý event đến muộn |
| Latency target | **p95 `published_at` → hiển thị trên dashboard ≤ 10 phút** trong phép đo demo |
| Backfill | Manual backfill/re-run có watermark, không double count |
| Local deployment | Docker Compose, tài liệu dựng lại end-to-end |

Latency target là **acceptance target cần kiểm chứng**, không phải kết quả đã đạt. Đo với >=100 live events, ghi số đo thực tế (p50, p95, sample size, điều kiện demo).

## 9. Data Quality, Governance & Risks

### 9.1. Tối thiểu 8 nhóm DQ bắt buộc

1. PK/business key không null, không trùng; `event_id` unique.
2. Referential integrity giữa patient/encounter/department/bed/lab order.
3. Trạng thái hợp lệ và transition hợp lệ.
4. Timestamp ordering (check-in trước bắt đầu khám, admission trước discharge, order trước result...).
5. Không cấp một giường cho nhiều encounter cùng thời điểm.
6. Không cấp nhiều giường đồng thời cho một inpatient encounter.
7. Bed status history không overlap; occupancy không vượt số giường ACTIVE.
8. Reconciliation + freshness (OLTP↔DWH; event↔raw/mart).

Kiểm thử thêm: chạy lại DAG không nhân đôi records, late/duplicate event, xử lý quarantine và kiểm tra dữ liệu KPI mẫu bằng tay.

### 9.2. Privacy & Security

Chỉ dùng synthetic data; không dùng họ tên/số định danh thật. Secrets trong `.env` không commit lên Git. Metabase dùng tài khoản đọc-only phù hợp, không expose DB công khai trên Internet.

### 9.3. Rủi ro và giảm thiểu

| Rủi ro | Giảm thiểu |
|---|---|
| Scope quá rộng | Giữ đúng 3 domain lõi, 11 KPI; 4 KPI nâng cao là optional |
| Không đủ RAM khi chạy full stack | Tinh giản số lượng container / chạy theo lượt; giảm cỡ test dataset |
| KPI sai do dữ liệu thời gian | Chốt timezone, temporal interval, test case ngay từ Phase 1 |
| Event mất/trùng/do sai thứ tự | Dedupe `event_id`, retry, reconcile, eventual correction |
| Batch và streaming double count | Key thống nhất và source-of-truth/reconciliation rõ ràng |

## 10. Analytics / Dashboard Scope

| Dashboard | KPI/nội dung chính |
|---|---|
| 1. **Executive Overview** | OP01, IP01, IP02, LAB01, các xu hướng theo ngày/khoa |
| 2. **Outpatient Operations** | OP01, OP02, OP03; giờ cao điểm, khoa |
| 3. **Inpatient & Bed Management** | IP01, IP02, IP03; nhập/xuất viện, occupancy |
| 4. **Laboratory & Real-Time** | LAB01, LAB02, LAB03, RT01, RT02 |

Có bộ lọc phù hợp theo ngày/khoa/loại xét nghiệm. Ngoài dashboard nghiệp vụ phải có truy vấn/trang kiểm tra freshness, số records lỗi và reconciliation.

## 11. Roadmap tổng quan

| Phase | Tên | Deliverables chính |
|---|---|---|
| **0** | Project Definition | Project Charter, scope, KPI, kiến trúc và baseline — **đã chốt** |
| **1** | Data & Dimensional Modeling | Source ERD, data dictionary, grain, star schema, KPI mapping, event contracts, DQ specs |
| **2** | Foundation & Synthetic Data | Docker, OLTP schema, generator, ingestion raw/stg |
| **3** | Batch DW & Historical BI | Airflow batch, dimensional warehouse, historical KPI, dashboard |
| **4** | Kafka & Near Real-Time BI | Producer/consumer, raw events, realtime mart, RT dashboard |
| **5** | Integration, Testing & Demo | DQ/latency/duplicate tests, documentation, demo |

**Dự trù:** 6–8 tuần (chưa gắn với lịch thực tế). Nếu nhóm có 4 thành viên, gợi ý phân công: (A) modeling & KPI, (B) source & synthetic data, (C) Airflow/Kafka, (D) DQ/dashboard/docs; tất cả review cùng một contract.

## 12. Acceptance Criteria — V2.0

- [ ] Có ERD OLTP, dimensional star schema, data dictionary, source-to-target mapping.
- [ ] Dựng lại toàn hệ thống local từ README/Docker Compose.
- [ ] Sinh được dữ liệu synthetic 12 tháng đúng ba domain và các tình huống ngoại lệ.
- [ ] Trả lời **11 KPI bắt buộc** theo công thức chính thức; có test case đối chiếu.
- [ ] Airflow incremental chạy được, retry và re-run/backfill không nhân đôi KPI.
- [ ] Kafka producer/consumer hoạt động trên **3 topics**; event duplicate/late được xử lý.
- [ ] Ít nhất **8 nhóm DQ checks**, có bằng chứng log/report.
- [ ] Có **4 dashboard**, cho thấy historical và near-real-time analytics.
- [ ] Thử >=100 sự kiện live và báo cáo p50/p95 latency thực tế so với target ≤10 phút.
- [ ] Có sơ đồ kiến trúc, vận hành, hướng dẫn dựng lại, giới hạn, kết quả test và demo script.

## 13. Phase 0 Exit / Handover

**Phase 0 được khóa thành baseline v1.0.** Phase 1 nhận đầu vào là:

1. Integrated Patient Journey + business rules ba domain;
2. KPI Catalog 11 bắt buộc, 4 tùy chọn;
3. 10 nguồn OLTP, 3 Kafka topics;
4. Yêu cầu batch/stream, chất lượng, kiến trúc và acceptance.

**TBD mang tính quản lý (không chặn thiết kế Phase 1):** tên thành viên/nhóm, tài nguyên máy cá nhân, deadline môn học cụ thể. Không tự coi các giả định 4 người / 6–8 tuần là dữ kiện được xác nhận.

**Quy tắc change control:** nếu thay đổi KPI, source entities, grain hoặc phạm vi, cập nhật tài liệu baseline (version + change log) và kiểm tra tác động tới model, generator, DAG và dashboard.

### Change Log

| Date | Version | Change |
|---|---|---|
| 2026-10-22 | 1.0 | Khóa 12 quyết định Phase 0; chuẩn hóa KPI/architecture/source-of-truth; bàn giao Phase 1 |
