# Scikit learn
from sklearn.cluster import KMeans
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, normalize
from sklearn.metrics import pairwise_distances_argmin
# Autres
from collections import defaultdict
import pandas as pd
import numpy as np
from copy import deepcopy
from itertools import product
from datetime import timedelta, date
import os
import json


# ----------------
# Hourly regression model for the deterministic component
# ----------------
class HourlyReg:

    def __init__(self, model=LinearRegression()):
        self.model = model
        self.regressors_name = None
        self.target_name = None

    def calibration(self, target, regressors):
        # Regression to build the hourly PFC
        self.regressors_name = regressors.columns.values
        self.target_name = target.columns
        self.model.fit(regressors.to_numpy(), target.to_numpy())

    def generate(self, regressors):
        return pd.DataFrame(data=self.model.predict(regressors[self.regressors_name].to_numpy()),
                            index=regressors.index, 
                            columns=self.target_name)
    
    def save(self, dir):
        os.makedirs(os.path.join(dir, 'regression'), exist_ok=True)
        np.save(os.path.join(dir, 'regression', 'coef_.npy'), self.model.coef_)
        np.save(os.path.join(dir, 'regression', 'intercept_.npy'), self.model.intercept_)
        names = {"target" : list(self.target_name),
                 "regressors" : list(self.regressors_name)}
        with open(os.path.join(dir, 'regression', 'names.json'), 'w') as f:
            json.dump(names, f, indent=4)

    def load(self, dir):
        self.model.coef_ = np.load(os.path.join(dir, 'regression', 'coef_.npy'))
        self.model.intercept_ = np.load(os.path.join(dir, 'regression', 'intercept_.npy'))
        with open(os.path.join(dir, 'regression', 'names.json'), 'r') as f:
            names = json.load(f)
        self.target_name = names['target']
        self.regressors_name = names['regressors']


# ----------------
# CRE model for the deterministic component
# ----------------
class CRE:

    def __init__(self, model=LinearRegression()):
        self.model = model
        self.dummies = None

    def calibration(self, spot, regressors):
        # Get the hourly shape for typical days
        self.dummies = self._dummies_calibration(spot)
        # Compute daily normalised PFC
        daily_pfc = self._to_daily_pfc(spot)
        # Regression to build the daily PFC
        self.model.fit(regressors.iloc[:, 1:].to_numpy(), daily_pfc["DA (€/MWh)"].to_numpy())

    def generate(self, timestamp, regressors):
        # Initialisation
        pfc_norm = []
        # Generate daily PFC
        daily_pfc = pd.DataFrame(data={"Timestamp": regressors["Timestamp"],
                                       "DA (€/MWh)": self.model.predict(regressors.iloc[:, 1:].to_numpy())})
        # To speed up computation, transformation into dict
        aux_d = daily_pfc.groupby(
            [daily_pfc["Timestamp"].dt.day, daily_pfc["Timestamp"].dt.month, daily_pfc["Timestamp"].dt.year])[
            "DA (€/MWh)"].apply(lambda x: x).droplevel(level=3).to_dict()
        aux_h = self.dummies.groupby(["Hour", 'Day', 'Month'])["DA (€/MWh)"].apply(lambda x: x).droplevel(
            level=3).to_dict()
        # Generate the hourly normalised PFC by multiplying the daily and hourly PFCs
        for date in timestamp:
            d = aux_d[date.day, date.month, date.year]
            h = aux_h[date.hour, date.dayofweek, date.month]
            pfc_norm.append(d * h)
        return pd.DataFrame(data={"Timestamp": timestamp, "DA (€/MWh)": pfc_norm})

    @staticmethod
    def _to_daily_pfc(spot):
        # Daily mean
        daily_pfc = spot.resample('D', on="Timestamp").mean().reset_index()
        # Normalisation by the mean
        daily_pfc.loc[:, "DA (€/MWh)"] = daily_pfc["DA (€/MWh)"] / daily_pfc.groupby(daily_pfc["Timestamp"].dt.year)[
            "DA (€/MWh)"].transform('mean')
        return daily_pfc

    @staticmethod
    def _dummies_calibration(spot):
        # Compute the mean for each typical days
        dummies = spot.groupby(
            [spot["Timestamp"].dt.hour, spot["Timestamp"].dt.dayofweek, spot["Timestamp"].dt.month]).mean().rename_axis(
            index=["Hour", "Day", "Month"], ).reset_index().drop(columns=["Timestamp"])
        # Normalisation by the mean
        for d in spot["Timestamp"].dt.dayofweek.unique():
            for m in spot["Timestamp"].dt.month.unique():
                dummies.loc[(dummies["Day"] == d) & (dummies["Month"] == m), "DA (€/MWh)"] = dummies.loc[
                    (dummies["Day"] == d) & (dummies["Month"] == m), "DA (€/MWh)"].div(
                    dummies.loc[(dummies["Day"] == d) & (dummies["Month"] == m), "DA (€/MWh)"].mean())
        return dummies


# ----------------
# Markov-Chain model for the stochastic residual component
# ----------------
class MarkovChain:

    def __init__(self, n_states=100, isweekend=True):
        self.intraday_transition_matrices = None
        self.extraday_transition_matrices = None
        self.states = None
        self.n_states = n_states
        self.isweekend = isweekend
        self.column_names = None

    def calibration(self, train_data, scaler=StandardScaler()):
        # Initialisation 
        self.states = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self.scaler = scaler
        self.intraday_transition_matrices = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self.extraday_transition_matrices = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 
                                                defaultdict(lambda: np.zeros(shape=(self.n_states, self.n_states), 
                                                                             dtype=np.float64)))))
        self.column_names = list(train_data.columns)
        # Normalisation
        train_data.iloc[:, :] = self.scaler.fit_transform(train_data)
        # Transform time series into cluster index time sequences
        sequences_df = train_data.groupby([train_data.index.month,
                                           (train_data.index.dayofweek >= 5) & self.isweekend, 
                                           train_data.index.hour]).apply(self._to_states_ts)
        sequences_df.index = sequences_df.index.set_names(['Month', 'Weekend', 'Hour', 'Date'])
        # Daily sequences
        daily_sequences_df = sequences_df.pivot_table(index=['Month', 'Weekend', 'Date'],
                                                      columns='Hour',
                                                      values='Cluster')
        # Calculation of intraday transition matrices
        for m, d in product(*sequences_df.index.levels[:2]):
            subset_df = daily_sequences_df.loc[(m, d)]
            for h in range(0, 23):
                transitions_df = subset_df[[h, h+1]].dropna().astype(int)
                transition_matrix = transitions_df.pivot_table(index=h, columns=h+1, aggfunc='size', fill_value=0)
                transition_matrix = transition_matrix.reindex(index=np.arange(self.n_states), 
                                                              columns=np.arange(self.n_states),
                                                              fill_value=0).to_numpy()
                self.intraday_transition_matrices[m][int(d)][h] = normalize(transition_matrix, axis=1, norm='l1')
        # Calculation of transition matrices between days
        daily_df = daily_sequences_df.reset_index(level=[0, 1])
        last_hours_df = daily_df[[23, 'Month', 'Weekend']]
        last_hours_df.index = last_hours_df.index.map(lambda x: x+timedelta(days=1))
        first_hours_df = daily_df[[0, 'Month', 'Weekend']].rename(columns={'Month' : 'Next_month', 'Weekend' : 'Next_weekend'})
        btw_transitions_df = pd.concat([last_hours_df, first_hours_df], axis=1).dropna().astype(int)
        for (m, next_m, d, next_d), df in btw_transitions_df.groupby(['Month', 'Next_month', 'Weekend', 'Next_weekend']):
            transition_matrix = df.pivot_table(index=23, columns=0, aggfunc='size', fill_value=0)
            transition_matrix = transition_matrix.reindex(index=np.arange(self.n_states), 
                                                          columns=np.arange(self.n_states),
                                                          fill_value=0).to_numpy()
            self.extraday_transition_matrices[m][d][next_m][next_d] = normalize(transition_matrix, axis=1, norm='l1')

    def _to_states_ts(self, df):
        m, d, h = df.name
        kmeans = KMeans(n_clusters=self.n_states, random_state=0).fit(df.to_numpy())
        self.states[m][d][h] = self.scaler.inverse_transform(kmeans.cluster_centers_)
        return pd.DataFrame(data=kmeans.predict(df.to_numpy()),
                            index=df.index.date,
                            columns=['Cluster'])

    def generate(self, timestamp, nscenario, sp_0=None):
        if sp_0 is None:
            sp_0 = [0 for _ in range(len(self.column_names))]
        first_cluster = pairwise_distances_argmin([sp_0], self.states[timestamp[0].month][int((timestamp[0].dayofweek >= 5) & self.isweekend)][timestamp[0].hour])[0]
        clusters_t = np.array([first_cluster for _ in range(nscenario)])
        sequence = list()
        for t in timestamp:
            m, d, h = t.month, int((t.dayofweek >= 5) & self.isweekend), t.hour
            sequence.append(self.states[m][d][h][clusters_t, :])
            if h < 23:
                transition_matrix = self.intraday_transition_matrices[m][d][h]
                transition_lines = transition_matrix[clusters_t, :]
                random_values = np.random.rand(nscenario, 1)
                clusters_t = (np.cumsum(transition_lines, axis=1) > random_values).argmax(axis=1)
            else:
                next_t = t + pd.Timedelta(hours=1)
                if t.year != next_t.year:
                    next_cluster = pairwise_distances_argmin([sp_0], self.states[1][int((next_t.dayofweek >= 5) & self.isweekend)][0])[0]
                    clusters_t = np.array([next_cluster for _ in range(nscenario)])
                else: 
                    next_m = next_t.month
                    next_d = int(next_t.dayofweek >= 5) & self.isweekend
                    transition_matrix = self.extraday_transition_matrices[m][d][next_m][next_d]
                    transition_lines = transition_matrix[clusters_t, :]
                    random_values = np.random.rand(nscenario, 1)
                    clusters_t = np.where(transition_lines.sum(axis=1) != 0,
                                         (np.cumsum(transition_lines, axis=1) > random_values).argmax(axis=1),
                                         pairwise_distances_argmin(sequence[-1], self.states[next_m][next_d][0]))
        # To dataframe
        if len(self.column_names) == 1:
            return np.array(sequence)[:,:,0], self.column_names
        else:
            return np.array(sequence), self.column_names


# ----------------
# Ornstein-Uhlenbeck model
# ----------------
class OrnsteinUlhenbeck:
    def __init__(self, ou_estimation="ols"):
        self.kappa = None
        self.sigma = None
        self.mu = None
        self.ou_estimation = ou_estimation

    def calibration(self, x):
        if self.ou_estimation == "ols":
            self.ols_estimation(x)
        elif self.ou_estimation == "mle":
            self.mle_estimation(x)

    def ols_estimation(self, x, dt=1):
        # Initialisation
        Y, X = x[1:], x[:-1].reshape(-1, 1)
        # OLS
        model = LinearRegression()
        model.fit(X, Y)
        # Compute model parameters
        self.kappa = - np.log(model.coef_[0]) / dt
        self.mu = model.intercept_ / (1 - model.coef_[0])
        self.sigma = np.std(Y - model.predict(X)) * np.sqrt(2 * self.kappa / (1 - model.coef_[0] ** 2))

    def mle_estimation(self, x, dt=1):
        N = len(x)
        # Compute dummy variables
        Sx = sum(x[:-1])
        Sy = sum(x[1:])
        Sxx = sum(x[i] ** 2 for i in range(N - 1))
        Sxy = sum(x[i] * x[i + 1] for i in range(N - 1))
        Syy = sum(x[i + 1] ** 2 for i in range(N - 1))
        # Compute model parameters
        self.mu = (Sy * Sxx - Sx * Sxy) / (N * (Sxx - Sxy) - (Sx ** 2 - Sx * Sy))
        self.kappa = -(1 / dt) * np.log((Sxy - self.mu * Sx - self.mu * Sy + N * self.mu ** 2) / (Sxx - 2 * self.mu * Sx + N * self.mu ** 2))
        sigma2_hat = (
                             Syy
                             - 2 * np.exp(-self.kappa * dt) * Sxy
                             + np.exp(-2 * self.kappa * dt) * Sxx
                             - 2 * self.mu * (1 - np.exp(-self.kappa * dt)) * (Sy - np.exp(-self.kappa * dt) * Sx)
                             + N * self.mu ** 2 * (1 - np.exp(-self.kappa * dt)) ** 2
                     ) / N
        self.sigma = np.sqrt(sigma2_hat * 2 * self.kappa / (1 - np.exp(-2 * self.kappa * dt)))

    def simulate_OU_process(self, timestamp, dt=1):
        # Initialisation
        N = int(len(timestamp) / dt)
        Y = [0] * N
        Y[0] = np.random.normal(0, np.sqrt(self.sigma ** 2 / (2 * self.kappa) * (1 - np.exp(-2 * self.kappa * dt))))
        W = np.sqrt(dt) * np.random.randn(N)
        # Process
        for t in range(N - 1):
            Y[t + 1] = self.mu + (Y[t] - self.mu) * np.exp(-self.kappa * dt) + np.sqrt(
                self.sigma ** 2 / (2 * self.kappa) * (1 - np.exp(-2 * self.kappa * dt))) * W[t]
        return Y


def compute_deltaTS(deltaT, TN):
    # Real temperature to compute the threshold
    TR = [deltaT[i] + TN[i] for i in range(len(deltaT))]
    # Compute deltaT including threshold
    deltaTS = [deltaT[i] * (TR[i] <= 15) for i in range(len(deltaT))]
    return deltaTS


def build_regressors(timestamp, days=True, months=True, hours=True, **kwargs):
    # Rename columns and concatenate into a single DataFrame
    series_list = []
    for k, v in kwargs.items():
        if type(v) == pd.Series:
            series_list.append(v.rename(k))
        elif type(v) == pd.DataFrame:
            if len(v.columns) == 1:
                series_list.append(v[v.columns[0]].rename(k))
            else:
                raise Exception(f"More than one column in {k} input")
        else:
            raise Exception(f"{k} input is not a Series nor a DataFrame")
    try:
        regressors = pd.concat(series_list, axis=1)
        # Reindex with Spot's Index
        regressors.reindex(timestamp)
    except ValueError:
        regressors = pd.DataFrame(index=timestamp)
    # Dummies for days
    if days:
        regressors['day'] = regressors.index.dayofweek.map(lambda x: f'D{x}')
    # Dummies for months
    if months:
        regressors['month'] = regressors.index.month.map(lambda x: f'M{x}')
    # Dummies for hours
    if hours:
        regressors['hours'] = regressors.index.hour.map(lambda x: f'H{x}')
    prefix = ['' for e in [days, months, hours] if e]
    if len(prefix) > 0:
        regressors = pd.get_dummies(regressors, prefix=['', '', ''], prefix_sep='' , dtype=int)
    return regressors


def remove_outliers(df, std_lim=3):
    # Comput the average 
    mean = df.mean()
    # Positive/negative outliers masks
    mask_outliers_pos = df - mean > std_lim * df.std() 
    mask_outliers_neg = df - mean < -std_lim * df.std() 
    # Non-outliers mask
    mask = ~mask_outliers_pos & ~mask_outliers_neg
    # Replace extreme values by the previous day's hourly value or by the average for the first day
    spot_filtered = df.where(mask, pd.NA).groupby(df.index.hour).ffill().fillna(mean)
    # Retrieve positive/negative outliers
    outliers_pos = df.where(mask_outliers_pos, 0)
    outliers_neg = df.where(mask_outliers_neg, 0)

    return spot_filtered, outliers_pos, outliers_neg


def smoothing(pfc, sampling_period='W', rolling_window=6):

    # Smooth means for the given sampling period
    sample_means = pfc.resample(sampling_period, label='left').mean()
    # smoothed_sample_means =  sample_means.rolling(rolling_window, center=True, min_periods=1).mean()
    smoothed_sample_means = sample_means.rolling(rolling_window, center=True).mean().fillna(sample_means)
    sample_factors = smoothed_sample_means / sample_means
    pfc_smooth = pfc * sample_factors.reindex(pfc.index, method='ffill')
    # Correct monthly means
    monthly_means_pfc = pfc.resample('MS').mean()
    monthly_means_pfc_smooth = pfc_smooth.resample('MS').mean()
    monthly_factors = monthly_means_pfc / monthly_means_pfc_smooth
    pfc_smooth = pfc_smooth * monthly_factors.reindex(pfc_smooth.index, method='ffill')
    
    return pfc_smooth


def process_eex_df(eex):
    # Modify the DataFrame of future quotations to make it easier to carry out subsequent operations
    eex = eex[eex.tenor.isin(['Year', 'Quarter', 'Month'])].copy()
    # Preference for the middle of the delivery interval because there is sometimes inaccuracy on the bounds
    eex['middle'] = eex.delivery_start + (eex.delivery_end - eex.delivery_start)/2
    eex['delivery_year'] = eex.middle.dt.year
    # Specify its tenor index for each product. Enable to perform immediate grouping operations
    eex.loc[eex.tenor == 'Month', 'product_index'] = eex.loc[eex.tenor == 'Month', 'middle'].dt.month
    eex.loc[eex.tenor == 'Quarter', 'product_index'] = eex.loc[eex.tenor == 'Quarter', 'middle'].dt.quarter
    eex.loc[eex.tenor == 'Year', 'product_index'] = 1
    eex.product_index= eex.product_index.astype(int)
    # Remove columns that are no longer needed
    return eex.drop(columns=['delivery_start', 'delivery_end', 'middle'])


def get_hist_quotations(eex):
    # Compute mean quotations for each product of each trading year
    M_and_Q_quot = eex[eex.tenor.isin(['Quarter', 'Month']) & eex.delivery_year.isin(eex.trading_date.dt.year.unique())].copy()
    avg_annual_prices = M_and_Q_quot.groupby(['tenor', 'type', 'delivery_year', 'product_index'])[['settlement_price']].mean()
    # Normalize monthly and quarterly products
    normalized_avg_annual_prices = avg_annual_prices.groupby(level=[0, 1, 2]).transform(lambda x: x / x.mean())
    # Compute mean quotations for each product over the year
    avg_prices = normalized_avg_annual_prices.groupby(level=[0, 1, 3]).mean()
    # Normalize monthly and quarterly products
    return avg_prices.groupby(level=[0, 1]).transform(lambda x: x / x.mean())


def parse_quotations(eex, delivery_years, trading_date):
    # Retrieve quotations for the given year on the specified trading date
    desired_quot = eex[(eex.delivery_year.isin(delivery_years)) & (eex.trading_date.dt.date == trading_date)]
    # Put in an easy-to-use format
    return desired_quot.groupby(['delivery_year', 'tenor', 'type', 'product_index'])[['settlement_price']].mean()


def get_adjusted_monthly_quotation(eex, trading_date, years, M_Q_hist, hourly_timestamp, return_quot=False):
    # Get quotations for given years on the specified trading date
    quot = parse_quotations(eex, years, trading_date)
    # Re-index quotations (and historical ones to add the year level) with pd.NA for those not available
    new_index = pd.MultiIndex.from_tuples(
        [(year,) + index for year, index in product(years, M_Q_hist.index)],
        names=['delivery_year'] + M_Q_hist.index.names
    )
    reindexed_quot = quot.reindex(new_index)
    M_Q_hist_reindexed = pd.DataFrame(index=new_index, data=np.tile(M_Q_hist.to_numpy(), (len(years), 1)), columns=['settlement_price'])
    # Recover the quotation for the calendar product and reindex for subsequent operations
    year_quot = quot.xs('Year', level='tenor').droplevel('product_index').groupby('delivery_year').apply(
        lambda x: x.droplevel(0).reindex(M_Q_hist.index, level='type'))
    # Compute multiplication coefficients to be applied to the historical quotations 
    # for non-listed products to keep the annual average equal to the calendar quotation
    coef = (year_quot - reindexed_quot.fillna(0)).groupby(['delivery_year', 'tenor', 'type']).sum().div(
        M_Q_hist_reindexed[reindexed_quot.isna()].groupby(['delivery_year', 'tenor', 'type']).transform('sum')) 
    coef.replace(to_replace=[np.inf, -np.inf], value=0, inplace=True)
    # Fill pd.NA with adjusted historical quotations 
    completed_quot = reindexed_quot.fillna(M_Q_hist_reindexed * coef)
    month_quot = completed_quot.xs('Month', level='tenor')
    quarter_quot = completed_quot.xs('Quarter', level='tenor')
    # Normalize monthly quotations for each quarter
    normalized_month_quot = month_quot.groupby(['delivery_year', 'type', (month_quot.index.get_level_values(2) - 1) // 3 + 1]).transform(
        lambda x: x / x.mean())
    # Re-index quarterly quotations to match monthly ones
    quarter_quot.index = quarter_quot.index.set_levels((quarter_quot.index.levels[2] - 1) * 3 + 1, level=2)
    quarter_quot_reindexed = quarter_quot.reindex(month_quot.index, method='ffill')
    # Ajust monthly quotations to have quarterly averages equal to quarterly quotations
    adjusted_month_quot = normalized_month_quot * quarter_quot_reindexed
    # Generate a timeline of the year to calculate peak and offpeak hours
    timestamp_df = pd.DataFrame(index=hourly_timestamp, 
                                data={'delivery_year' : hourly_timestamp.year,
                                    'product_index' : hourly_timestamp.month})
    mask = (timestamp_df.index.hour > 7) & (timestamp_df.index.hour < 20) & (timestamp_df.index.dayofweek < 5)
    timestamp_df.loc[mask, 'type'] = 'peak'
    timestamp_df.loc[~mask, 'type'] = 'offpeak'
    nb_hour = timestamp_df.reset_index(names='settlement_price').groupby(['delivery_year', 'type', 'product_index']).count()
    # Compute offpeak monthly quotations noting that base quotations are weighted averages of peak and offpeak quotations
    offpeak_month_quot = ((nb_hour.xs('peak', level='type') + nb_hour.xs('offpeak', level='type')) * adjusted_month_quot.xs('base', level='type')).sub(
        nb_hour.xs('peak', level='type') * adjusted_month_quot.xs('peak', level='type')).div(
            nb_hour.xs('offpeak', level='type'))
    offpeak_month_quot.index = pd.MultiIndex.from_tuples([(y, 'offpeak', product_index) for y, product_index in offpeak_month_quot.index], 
                                                        names=adjusted_month_quot.index.names)
    if return_quot:
        return pd.concat([adjusted_month_quot, offpeak_month_quot]), quot
    else:
        return pd.concat([adjusted_month_quot, offpeak_month_quot]), None


def adjust_PFC_to_month_quot(norm_PFC, month_quot):
    pfc = norm_PFC.copy()
    # Generate month and peak/offpeak information for each timestamp
    pfc['delivery_year'] = pfc.index.year
    pfc['product_index'] = pfc.index.month
    mask = (pfc.index.hour > 7) & (pfc.index.hour < 20) & (pfc.index.dayofweek < 5)
    pfc.loc[mask, 'type'] = 'peak'
    pfc.loc[~mask, 'type'] = 'offpeak'
    # Normalize PFCs for every pair type, month
    noramlized_cols = pfc.groupby(['delivery_year', 'type', 'product_index']).apply(lambda x: x / x.mean().mean()).droplevel([0, 1, 2]).sort_index()
    pfc.loc[:, noramlized_cols.columns] = noramlized_cols.values
    # Merge monthly quotation informations for each timestamp and return the multiplication
    merged_df = pd.merge(left=pfc, left_on=['delivery_year', 'type', 'product_index'], right=month_quot, right_index=True)
    return merged_df.drop(columns=['delivery_year', 'type', 'product_index', 'settlement_price']).mul(merged_df.settlement_price, axis=0)