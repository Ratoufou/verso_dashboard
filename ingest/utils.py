import pandas as pd 
import datetime as dt

class DayCET():
    def __init__(self, date_str):
        self.date_str = date_str
        self.date = dt.date.fromisoformat(date_str)
        self.start_utc = pd.Timestamp(self.date).tz_localize('CET').tz_convert('UTC').tz_localize(None)
        self.end_utc = pd.Timestamp(self.date + dt.timedelta(days=1)).tz_localize('CET').tz_convert('UTC').tz_localize(None)

    def __str__(self):
        return f'{self.date_str} -> UTC interval : [{self.start_utc.strftime(format='%Y-%m-%d %H:%M')}, {self.end_utc.strftime(format='%Y-%m-%d %H:%M')}['