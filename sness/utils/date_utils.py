import pytz
from datetime import datetime


def build_today_partition(prefix, date_format="%Y-%m-%d", tz='UTC'):
    return prefix + datetime.now(tz=pytz.timezone(tz)).strftime(date_format) + '/'
