import lxml.etree as et
from isodate import parse_duration, duration_isoformat
import psycopg2
import pandas as pd
import os


EIC_CODES = {
    '10YFR-RTE------C' : 'FR'
}


def ENTSOE_DayAhead_process(date, country, dir, DB_CONFIG):

    file = f'{date}_DA_price_{country}.xml'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return None
    
    tree = et.parse(path)
    root = tree.getroot()
    ns = root.nsmap
    ts_elem = root.find('TimeSeries', namespaces=ns)
    country = EIC_CODES[ts_elem.find('in_Domain.mRID', namespaces=ns).text]
    unit = f"{ts_elem.find('currency_Unit.name', namespaces=ns).text}/{ts_elem.find('price_Measure_Unit.name', namespaces=ns).text}"
    period_elem = ts_elem.find('Period', namespaces=ns)
    time_inteval_elem = period_elem.find('timeInterval', namespaces=ns)
    start = pd.Timestamp(time_inteval_elem.find('start', namespaces=ns).text)
    end = pd.Timestamp(time_inteval_elem.find('end', namespaces=ns).text)
    resolution = parse_duration(period_elem.find('resolution', namespaces=ns).text)
    date_range = pd.date_range(start=start, end=end, freq=resolution, inclusive='left').tz_localize(None)
    val = [pd.NA for _ in range(len(date_range))]
    for pt in period_elem.findall('Point', namespaces=ns):
        pos = int(pt.find('position', namespaces=ns).text)
        price = float(pt.find('price.amount', namespaces=ns).text)
        val[pos-1] = price
    df = pd.DataFrame(data=val, index=date_range, columns=['price']).ffill()
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for timestamp, row in df.iterrows():
        cursor.execute(
            ('INSERT INTO elec_day_ahead_market ' 
             '(delivery_start, delivery_end, source, country, tenor, price, unit) ' 
             'VALUES (%s, %s, %s, %s, %s, %s, %s) '
             'ON CONFLICT (delivery_start, delivery_end, source, country) DO NOTHING'),

            (timestamp, 
             timestamp+resolution, 
             'ENTSOE', 
             country, 
             duration_isoformat(resolution), 
             round(float(row['price']), 2), 
             unit)
        )

    conn.commit()
    cursor.close()
    conn.close()
