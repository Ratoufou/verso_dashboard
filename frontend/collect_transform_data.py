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

def execute_query(query, timestamp=False, db_config=DB_CONFIG):
    conn = psycopg2.connect(**db_config) 
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    df =  pd.DataFrame(data=results, 
                       columns=[d.name for d in cursor.description])
    return df


def get_spot_prices():
    query = """
    SELECT 
        delivery_start, 
        delivery_end, 
        price 
    FROM elec_day_ahead_market 
    WHERE source = 'ENTSOE' 
    AND country = 'FR' 
    AND tenor = 'PT1H'
    ORDER BY delivery_start"""
    spot_df = execute_query(query)
    spot_df['delivery_start'] = pd.to_datetime(spot_df.delivery_start, utc=True).dt.tz_convert('CET').dt.tz_localize(None)
    spot_df['delivery_end'] = pd.to_datetime(spot_df.delivery_end, utc=True).dt.tz_convert('CET').dt.tz_localize(None)
    return spot_df


def get_futures_prices():
    query = """
    WITH middle_table AS (
        SELECT 
            trading_date,
            delivery_start::TIMESTAMP + (delivery_end::TIMESTAMP - delivery_start::TIMESTAMP)/2 AS middle,
            CASE
                WHEN peak THEN 'Peak'
                ELSE 'Base'
            END AS type,
            tenor,
            settlement_price
        FROM elec_futures_market 
        WHERE tenor IN ('Year', 'Quarter', 'Month') 
        AND source IN ('EEX', 'KPLER') 
        AND country = 'FR'
        AND trading_date < delivery_start
    ) SELECT
        trading_date,
        EXTRACT(YEAR FROM middle) AS delivery_year,
        tenor,
        CASE
            WHEN tenor = 'Year' THEN 1
            WHEN tenor = 'Quarter' THEN EXTRACT(QUARTER FROM middle)
            WHEN tenor = 'Month' THEN EXTRACT(MONTH FROM middle)
        END AS product_index,
        type,
        settlement_price
    FROM middle_table
    ORDER BY trading_date, delivery_year, tenor, type, product_index
    """
    futures_df = execute_query(query)
    return futures_df


def get_gas_prices():
    query = """
    SELECT 
        delivery_start, 
        last_price 
    FROM gas_day_ahead_market 
    WHERE source = 'KPLER' 
    AND country = 'FR' 
    AND tenor = 'Day' 
    ORDER BY delivery_start"""
    gas_df = execute_query(query).set_index('delivery_start')
    return gas_df


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


