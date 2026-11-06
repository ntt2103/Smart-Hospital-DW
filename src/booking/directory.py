"""Vietnamese display names for a simulated hospital; no real doctor affiliation."""
SPECIALTIES = ['Nội tổng quát', 'Ngoại tổng quát', 'Tim mạch', 'Nhi khoa',
               'Sản phụ khoa', 'Tai Mũi Họng', 'Mắt', 'Da liễu',
               'Cơ Xương Khớp', 'Tiêu hóa']


def specialty(identifier):
    return SPECIALTIES[(identifier - 1) % len(SPECIALTIES)]


def doctor_name(identifier):
    families = ['Nguyễn', 'Trần', 'Lê', 'Phạm', 'Hoàng', 'Vũ', 'Đặng', 'Bùi', 'Đỗ', 'Hồ']
    middles = ['Minh', 'Thanh', 'Ngọc', 'Quang', 'Gia', 'Khánh']
    names = ['An', 'Bình', 'Châu', 'Dũng', 'Giang', 'Hà', 'Hùng', 'Linh', 'Nam', 'Phương']
    index = identifier - 1
    title = ['GS. TS.', 'PGS. TS.', 'TS. BS.', 'ThS. BS.', 'BS. CKII', 'BS. CKI'][index % 6]
    return f'{title} {families[index % 10]} {middles[(index // 10) % 6]} {names[(index // 60) % 10]}'
