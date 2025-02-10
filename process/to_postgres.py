import os 
import process_scripts
import pandas as pd
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument('--data', dest='data_path', type=str)
parser.add_argument('--start', type=str)
parser.add_argument('--end', type=str)
args = parser.parse_args()

DB_CONFIG = {
    'host' : 'db',
    'database' : 'verso_database',
    'user' : 'root',
    'password' : 'versosql'
}

process_dict = {
    # 'ENTSOE/DayAhead' : process_scripts.ENTSOE_DayAhead_process,
    # 'KPLER/Futures/Elec/EEX' : process_scripts.KPLER_Futures_Elec_EEX_process,
    # 'KPLER/DayAhead/Gas/EEX' : process_scripts.KPLER_DayAhead_Gas_EEX_process,
    # 'RTE/Consumption' : process_scripts.RTE_Consumption_process,
    # 'RTE/Production' : process_scripts.RTE_Production_process,
    # 'RTE/Capacities' : process_scripts.RTE_Capacities_process
    'RTE/GenerationForecast' : process_scripts.RTE_GenerationForecast_process
}

for date in tqdm(pd.date_range(start=args.start, end=args.end, freq='D', inclusive='both').date):
    date_str = date.strftime(format='%Y%m%d')
    for dir, func in process_dict.items():
        for country in os.listdir(os.path.join(args.data_path, dir)):
            func(date_str, country, os.path.join(args.data_path, dir, country), DB_CONFIG)
