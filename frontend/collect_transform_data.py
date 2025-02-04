import pandas as pd
import psycopg2
import numpy as np
from datetime import timedelta

DB_CONFIG = {
    'host' : 'db',
    'database' : 'verso_database',
    'user' : 'root',
    'password' : 'versosql'
}

def execute_query(query, db_config=DB_CONFIG):
    conn = psycopg2.connect(**db_config) 
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return pd.DataFrame(data=results, 
                        columns=[d.name for d in cursor.description])


def get_spot_prices():
    query = 'SELECT * FROM elec_day_ahead_market'
    spot_df = execute_query(query)
    spot_df['delivery_start'] = pd.to_datetime(spot_df.delivery_start, utc=True).dt.tz_convert('CET').dt.tz_localize(None)
    spot_df['delivery_end'] = pd.to_datetime(spot_df.delivery_end, utc=True).dt.tz_convert('CET').dt.tz_localize(None)
    return spot_df


def get_futures_prices():
    query = 'SELECT * FROM elec_futures_market'
    futures_df = execute_query(query)
    futures_df = futures_df[futures_df.tenor.isin(['Year', 'Quarter', 'Month'])]
    futures_df = futures_df[futures_df.trading_date < futures_df.delivery_start]
    futures_df['middle'] = pd.to_datetime(futures_df.delivery_start + (futures_df.delivery_end - futures_df.delivery_start)/2)
    futures_df['delivery_year'] = futures_df.middle.dt.year
    futures_df.loc[futures_df.tenor == 'Month', 'product_index'] = futures_df.loc[futures_df.tenor == 'Month', 'middle'].dt.month
    futures_df.loc[futures_df.tenor == 'Quarter', 'product_index'] = futures_df.loc[futures_df.tenor == 'Quarter', 'middle'].dt.quarter
    futures_df.loc[futures_df.tenor == 'Year', 'product_index'] = 1
    futures_df.product_index = futures_df.product_index.astype(int)
    futures_df['type'] = np.where(futures_df.peak, 'Peak', 'Base')
    return futures_df.drop(columns=['delivery_start', 'delivery_end', 'middle', 'peak'])


def get_gas_prices():
    query = 'SELECT * FROM gas_day_ahead_market'
    gas_df = execute_query(query)
    return gas_df


def filter_gas_df(gas_df):
    filtered_gas_df = gas_df[gas_df.tenor == 'Day'][['delivery_start', 'last_price']].set_index('delivery_start').sort_index()
    return filtered_gas_df


def gather_tenor_info(futures_df, spot_df):
    futures_df.sort_values(by='trading_date', inplace=True)
    last_prices = futures_df.groupby(['delivery_year', 'tenor', 'product_index', 'type'])[['settlement_price']].last()
    last_prices = last_prices.pivot_table(index=['delivery_year', 'tenor', 'product_index'],
                                          columns=['type'],
                                          values='settlement_price')
    spot_tenor_df = spot_df.copy()
    spot_tenor_df['Month'] = spot_tenor_df.delivery_start.dt.month
    spot_tenor_df['Year'] = 1
    spot_tenor_df['Quarter'] = spot_tenor_df.delivery_start.dt.quarter
    spot_tenor_df['delivery_year'] = spot_tenor_df.delivery_start.dt.year
    spot_tenor_df['Base'] = True
    peak_mask = (spot_tenor_df.delivery_start.dt.hour > 7) & (spot_tenor_df.delivery_start.dt.hour < 20) & (spot_tenor_df.delivery_start.dt.weekday < 5)
    spot_tenor_df.loc[peak_mask, 'Peak'] = True
    spot_tenor_df = spot_tenor_df.melt(
        id_vars=['delivery_start', 'delivery_end', 'price', 'delivery_year', 'Base', 'Peak'],
        value_vars=['Year', 'Quarter', 'Month'],
        var_name='tenor',
        value_name='product_index'
    )
    spot_tenor_df = spot_tenor_df.melt(
        id_vars=['delivery_start', 'delivery_end', 'price', 'delivery_year', 'tenor', 'product_index'], 
        value_vars=['Base', 'Peak'],
        var_name='type',
        value_name='to_drop').dropna().drop(columns=['to_drop'])
    spot_tenor_df = spot_tenor_df.groupby(['delivery_year', 'tenor', 'product_index', 'type']).agg({
        'delivery_start' : 'min',
        'delivery_end' : 'max',
        'price' : 'mean'
    }).pivot_table(
        index=['delivery_year', 'tenor', 'product_index'],
        columns=['type'],
        values=['price', 'delivery_start', 'delivery_end']
    )[[('delivery_start', 'Base'), ('delivery_end', 'Base'), ('price', 'Base'), ('price', 'Peak')]]
    spot_tenor_df.columns = ['delivery_start', 'delivery_end', 'Spot_Base', 'Spot_Peak']
    return pd.concat([spot_tenor_df, last_prices], axis=1).dropna()


def build_products_evolution_tab(futures_df):
    today = futures_df.trading_date.max()
    yesterday = today - timedelta(days=1)
    month_products = {(f'M+{i}', type): (today.year + (today.month+i-1)//12, 'Month', (today.month+i-1)%12+1, type) for i in range(1,4) for type in ['Base', 'Peak']}
    year_products = {(f'Y+{i}', type): (today.year + i, 'Year', 1, type) for i in range(1,4) for type in ['Base', 'Peak']}
    quarter_products = {(f'Q+{i}', type): (today.year + ((today.month-1)//3+1+i-1)//4, 'Quarter', ((today.month-1)//3+1+i-1)%4+1, type) for i in range(1,5) for type in ['Base', 'Peak']}
    products = month_products | year_products | quarter_products
    today_quotations = futures_df[futures_df.trading_date == today].groupby(['delivery_year', 'tenor', 'product_index', 'type'])[['settlement_price']].mean()
    yesterday_quotations = futures_df[futures_df.trading_date == yesterday].groupby(['delivery_year', 'tenor', 'product_index', 'type'])[['settlement_price']].mean()
    evolution = pd.concat([today_quotations, (today_quotations - yesterday_quotations)*100 / yesterday_quotations], axis=1)
    evolution.columns = ['Price (€/MWh)', '% Change']
    evolution = evolution.loc[products.values()]
    evolution.index = products.keys()

    return evolution.astype(float).round(1).reset_index(
        names=['Product', 'Type']
    ).pivot_table(
        values=['Price (€/MWh)', '% Change'],
        index='Product',
        columns=['Type']
    ).reset_index()


