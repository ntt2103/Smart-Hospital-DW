"""Reporting bounds come from the same frozen manifest as the dataset."""
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


def report_dates(manifest=None):
    manifest = manifest or os.environ.get('SYNTHETIC_MANIFEST')
    if manifest:
        data = json.loads(Path(manifest).read_text(encoding='utf-8')) if not isinstance(manifest, dict) else manifest
        data = data.get('manifest', data)
        start = data.get('report_start_date')
        end = data.get('report_end_date')
        if not start or not end:
            zone = ZoneInfo('Asia/Ho_Chi_Minh')
            start = datetime.fromisoformat(data['history_start'].replace('Z', '+00:00')).astimezone(zone).date().isoformat()
            end = (datetime.fromisoformat(data['as_of'].replace('Z', '+00:00')).astimezone(zone) - timedelta(microseconds=1)).date().isoformat()
    else:
        # Original immutable demo seed; Phase 5 supplies its own manifest.
        start, end = '2025-10-22', '2026-10-21'
    if date.fromisoformat(start) > date.fromisoformat(end):
        raise ValueError('Manifest report_start_date exceeds report_end_date')
    return start, end
