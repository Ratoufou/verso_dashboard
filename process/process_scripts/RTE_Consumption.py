import pandas as pd


def RTE_Consumption_process(date, country, dir, DB_CONFIG):
    resp_json = get_rte_data()
    df_ls = []
    for e in resp_json['short_term']:
        var_type = e['type']
        df = pd.DataFrame(e['values'])
        df['start_date'] = pd.to_datetime(df['start_date'], utc=True)
        df.drop(['end_date', 'updated_date'], inplace=True, axis=1, errors='ignore')
        df.rename(columns={'value' : var_type, 'start_date' : 'Timestamp'}, inplace=True)
        df.set_index('Timestamp', inplace=True, drop=True)
        df_ls.append(df.tz_localize(None))
    return pd.concat(df_ls, axis=1)