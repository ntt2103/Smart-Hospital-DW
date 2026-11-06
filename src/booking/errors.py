"""Shared API errors, including when service is launched with python -m."""


class BookingError(ValueError):
    def __init__(self, message, status=422):
        super().__init__(message)
        self.status = status
