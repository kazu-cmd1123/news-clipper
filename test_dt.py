import datetime
from dateutil import parser as date_parser
from email.utils import parsedate_to_datetime

def test_comparison():
    pub_str = "Thu, 30 Apr 2026 01:00:00 GMT"
    dt = parsedate_to_datetime(pub_str)
    print(f"dt: {dt} (tz: {dt.tzinfo})")
    
    # Simulate saving to DB and reading back
    dt_iso = dt.isoformat()
    print(f"ISO saved: {dt_iso}")
    
    since_dt = date_parser.parse(dt_iso)
    print(f"since_dt: {since_dt} (tz: {since_dt.tzinfo})")
    
    print(f"dt <= since_dt: {dt <= since_dt}")
    
    # Try another one
    pub_str_new = "Fri, 01 May 2026 01:00:00 GMT"
    dt_new = parsedate_to_datetime(pub_str_new)
    print(f"dt_new: {dt_new}")
    print(f"dt_new > since_dt: {dt_new > since_dt}")

if __name__ == "__main__":
    test_comparison()
