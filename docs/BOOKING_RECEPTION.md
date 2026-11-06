# Đặt lịch và tiếp nhận bệnh nhân

Form tại http://localhost:28081 đi theo thứ tự: họ tên → mục đích khám → chuyên khoa → bác sĩ → ngày giờ → cửa sổ kiểm tra → xác nhận.

Bốn mục đích theo thứ tự: Khám sức khỏe tổng quát; Tái khám theo lịch hẹn; Khám vì có triệu chứng; Khám sức khỏe định kỳ. Chọn tái khám bắt buộc nhập mã bệnh nhân đang hoạt động. Các lựa chọn còn lại dùng luồng đăng ký lần đầu.

Đặt trước giờ hẹn ít nhất 30 phút, tối đa 365 ngày. Server kiểm tra lại bằng đồng hồ nguồn khi nhận yêu cầu mới. Retry cùng UUID và cùng nội dung trả lại đăng ký cũ; UUID khác nội dung trả 409.

Người khám lần đầu được lưu tại `public.booking_registrations` cùng tên và mục đích khám. Mọi đăng ký tạo appointment SCHEDULED ngay lập tức. Với người mới, hệ thống tạo hồ sơ tạm mã TMP nội bộ để giữ liên kết; mã bệnh nhân chính thức BN chỉ cấp khi check-in. Quầy dùng mã đăng ký (UUID) để check-in qua `POST /api/registrations/{id}/check-in`. Khu vực demo quầy tiếp nhận nằm trong thẻ kết quả đăng ký trên form. Check-in cho phép từ 30 phút trước đến 30 phút sau giờ hẹn.

Check-in cấp mã bệnh nhân chính thức `BN-xxxxxxxx`, chuyển appointment SCHEDULED → ATTENDED và tạo encounter CHECKED_IN trong cùng một transaction. Ngày sinh chưa thu thập được giữ NULL, giới tính UNKNOWN; không tự bịa thông tin. Họ tên và mục đích được giữ trong bản đăng ký liên kết với patient_id. Luồng tái khám tạo appointment khi đăng ký và dùng lại patient_id tại check-in. Request row lock bảo vệ check-in đồng thời, không tạo thêm patient/encounter khi gửi lại.

`booking_event_outbox` giữ sự kiện trong cùng transaction với nghiệp vụ. Worker xuất Kafka và chỉ đánh dấu sau broker acknowledgement; consumer hiện có chống trùng event_id. Check-in phát sự kiện hợp đồng hiện có, nên near-RT job có thể tính RT01. Batch hiện có đưa các bảng patients/appointments/encounters vào DW; metadata đăng ký mới (họ tên/mục đích) chưa được thêm vào mô hình phân tích hoặc dashboard.

Chuyên khoa và tên bác sĩ trên form là danh mục hiển thị tiếng Việt trong `src/booking/directory.py`, ánh xạ vào mã khoa/bác sĩ nguồn hiện hữu. Đây là tên và học hàm/học vị minh họa, không phải hồ sơ bác sĩ thật. Các tên synthetic trong dữ liệu benchmark cũ không bị ghi đè.

Từ bản tái cấu trúc ngày 06/11/2026, chạy `docker compose up --build` tại thư mục gốc. Service `app-init` áp dụng migration có checksum `013_booking_reception` (OLTP), `014_noshow_30_minutes` (DWH) trước khi booking khởi động; không seed/load lại nguồn đã khởi tạo hoàn chỉnh. Compose dùng trực tiếp mã nguồn tại thư mục chính. Module kết nối và khởi tạo chuyển sang `operations.foundation`; không còn cần script `update_booking.ps1`.

Kiểm chứng: `tests/test_reception.py` dùng database tạm riêng (tạo và xóa database có tên UUID), kiểm tra first/returning/check-in, canonical events, biên 30 phút, concurrent retry, conflict và mã không tồn tại. `tests/test_booking_recovery.py` giữ các kiểm tra API cũ. `node tests/booking_ui_behavior.cjs` kiểm tra preview chưa gửi, trường mã tái khám, confirmation, retry cùng payload và validation giờ. Đã kiểm tra rendering form và modal trên trình duyệt; ảnh tại `artifacts/booking/confirmation.png`.

## Tự động ghi nhận vắng hẹn

Worker kiểm tra mỗi vòng (khoảng 3 giây khi dịch vụ sẵn sàng). Các appointment liên kết booking_registrations, còn SCHEDULED, chưa có encounter và đã quá giờ hẹn hơn 30 phút sẽ chuyển NO_SHOW. Không xóa bản đăng ký/lịch hẹn; không cấp mã BN hoặc tạo encounter cho người vắng mặt. Khóa row ngăn check-in và phân loại vắng hẹn ghi đè nhau. Sự kiện status_changed được lưu cùng transaction trong outbox, retry không tạo thêm sự kiện phân loại.

Các bản đăng ký lần đầu trước bản cập nhật được worker bổ sung hồ sơ tạm và appointment. Dữ liệu seed không bị tự phân loại lại. DQ nguồn, projection batch và CHECK fact_appointment cùng dùng grace 30 phút (migration 014). Batch vẫn phải chạy để đưa thay đổi vào fact/mart và KPI OP03; worker không trực tiếp viết DW. Form tự kiểm tra lại trạng thái lịch chờ mỗi 15 giây và khóa check-in khi NO_SHOW.

Kiểm chứng bổ sung NO_SHOW: 25 kiểm thử reception/recovery và 10 kiểm thử generator đạt; UI kiểm tra trạng thái NO_SHOW, khóa check-in và giữ bản ghi. Fixture fact/mart dùng đúng projection SQL sản xuất xác nhận một lịch vắng hẹn góp 1 vào no_show_count và 1 vào classified_count. Có kiểm thử check-in đồng thời tác vụ expire và bảo toàn lịch đã hủy/đã đến. Không thêm lịch quá hạn giả vào nguồn demo để làm minh chứng.
