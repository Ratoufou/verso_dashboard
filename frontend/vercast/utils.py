import pandas as pd
import datetime as dt
import os

# ----------------
# Generic forecast class
# ----------------
class Forecast:

    def __init__(self, data):
        self.data = data


def bdd_to_dataframe(years, path, day_to_hour=False, timestamp_index=True, **kwargs):

    if len(years) == 0:
        raise Exception(f'Empty list of years (bdd_to_dataframe)')
    index_col, parse_dates = (0, True) if timestamp_index else (None, False)
    # Open yearly files and concatenate data
    df_list = [pd.read_excel(os.path.join(path, f'{y}.xlsx'), **kwargs, index_col=index_col, parse_dates=parse_dates) for y in years]
    data = pd.concat(df_list, axis=0)
    # Resampling
    if day_to_hour:
        # Add one day to include completely the last day during the resampling
        last_day = data.index.max()
        data.loc[last_day + pd.Timedelta(days=1)] = data.loc[last_day]
        # Resample with forward propagation
        data = data.resample('h').ffill().iloc[:-1]
    return data
