import os
import psycopg2
import pandas as pd
import json
from isodate import duration_isoformat


def RTE_Consumption_process(date, country, dir, DB_CONFIG):

    file = f'{date}_RTE_Cons_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return None
    
    with open(path, 'r') as f:
        cons_dict = json.load(f)

    ts_dict = dict()
    for e in cons_dict['short_term']:
        cons_type = e['type']

        if cons_type == 'REALISED':
            if (cons_type == 'CORRECTED') or (len(e['values']) == 0):
                continue
            df = pd.DataFrame(e['values'])
            df.start_date = pd.to_datetime(df.start_date, utc=True).dt.tz_localize(None)
            df.end_date = pd.to_datetime(df.end_date, utc=True).dt.tz_localize(None)
            df['tenor'] = (df.end_date - df.start_date).apply(lambda x: duration_isoformat(x))
    
            if len(df[df.tenor == '-PT45M'].index.values) != 0:
                remove_index = [df[df.tenor == '-PT45M'].index.values[0] + i for i in range(-2,2)]
                df = df.drop(index=remove_index)
    
            if len(df[df.tenor == 'PT1H15M'].index.values) != 0:
                start = df[df.tenor == 'PT1H15M'].iloc[0]['start_date']
                val = df[df.tenor == 'PT1H15M'].iloc[0]['value']
                df = df.drop(index=df[df.tenor == 'PT1H15M'].index.values)
                added_lines = pd.DataFrame([{'start_date' : start + pd.Timedelta(minutes=15*i),
                                            'end_date' : start + pd.Timedelta(minutes=15*(i+1)),
                                            'value' : val,
                                            'tenor' : 'PT15M'} for i in range(5)])
                df = pd.concat([df, added_lines])
    
            ts_dict[cons_type] = df


    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for cons_type in ts_dict.keys():
        for _, row in ts_dict[cons_type].iterrows():
            cursor.execute(
                ('INSERT INTO consumption ' 
                '(cons_start, cons_end, source, country, tenor, curve_type, quantity, unit, processing) ' 
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (cons_start, cons_end, source, country, curve_type, processing) DO NOTHING'),

                (row.start_date, 
                 row.end_date, 
                 'RTE', 
                 country, 
                 row.tenor, 
                 cons_type,
                 round(float(row.value), 2), 
                 'MW',
                 'Unspecified')
            )
    
    conn.commit()
    cursor.close()
    conn.close()