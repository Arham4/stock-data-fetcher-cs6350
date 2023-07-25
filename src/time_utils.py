from datetime import datetime, timedelta
import pandas_market_calendars
import pandas as pd
from pytz import timezone

def get_current_date():
    current_date = datetime.now().strftime("%Y-%m-%d")
    return current_date

def get_date_for_days_before(base_date, days):
    base_date_obj = datetime.strptime(base_date, "%Y-%m-%d")
    new_date_obj = base_date_obj - timedelta(days=days)
    new_date_str = new_date_obj.strftime("%Y-%m-%d")
    return new_date_str

def get_date_for_days_after(base_date, days):
    base_date_obj = datetime.strptime(base_date, "%Y-%m-%d")
    new_date_obj = base_date_obj + timedelta(days=days)
    new_date_str = new_date_obj.strftime("%Y-%m-%d")
    return new_date_str

def is_date_before(date1, date2):
    date1_obj = datetime.strptime(date1, "%Y-%m-%d")
    date2_obj = datetime.strptime(date2, "%Y-%m-%d")
    return date1_obj < date2_obj

def parse_date(date):
    return datetime.strptime(date, "%Y-%m-%d")

def get_market_days(start_date, end_date):
    nyse_calendar = pandas_market_calendars.get_calendar('NYSE')
    schedule = nyse_calendar.schedule(start_date=start_date, end_date=end_date)
    market_days = pd.to_datetime(schedule.index)
    market_days_str = set(market_days.strftime('%Y-%m-%d'))
    return market_days_str

def is_market_day(date, market_days_set):
    return date in market_days_set

def is_dst(date_str):
    date_object = datetime.strptime(date_str, "%Y-%m-%d")
    nyc_tz = timezone('America/New_York')
    nyc_date = nyc_tz.localize(date_object)
    return nyc_date.dst() != timedelta(0)

def string_to_epoch_in_est(date_str):
    delta = 5
    if not is_dst(date_str):
        delta = 6
    datetime_obj = datetime.strptime(date_str, "%Y-%m-%d")
    datetime_obj_est = datetime_obj - timedelta(hours=delta) # EST is 5 hours behind UTC
    epoch_time = int(datetime_obj_est.timestamp())
    return epoch_time
