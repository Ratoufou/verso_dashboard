import pandas as pd
import numpy as np
from datetime import date
import os
import json
from vercast.middle_term import HourlyReg, MarkovChain, remove_outliers, smoothing, build_regressors, process_eex_df, get_adjusted_monthly_quotation, get_hist_quotations, adjust_PFC_to_month_quot
from vercast.utils import bdd_to_dataframe


def calibrate(spot, regressors, normalized_regressors, eval_years, name, residual_model_config, regressor_model_config, nscenario, output_path=None, save_files=True, sampling_period='W'):
    inputs = locals()
    del inputs['spot']
    del inputs['regressors']
    del inputs['normalized_regressors']

    # Models
    regression_model = HourlyReg()
    residual_model = MarkovChain(**residual_model_config)
    regressor_model = MarkovChain(**regressor_model_config)

    # Calibration 
    # Filtrage des outliers
    spot_filtered, outliers_pos, outliers_neg = remove_outliers(spot, std_lim=3)
    # Normalisation
    spot_filtered = spot_filtered / spot_filtered.groupby(spot_filtered.index.year).transform('mean')
    # Calibration de la composante de régression
    regression_model.calibration(target=spot_filtered, regressors=regressors)
    # Génération d'un scénario pour calculer le résidu et calibrer le modèle stochastique
    reg_pfc = regression_model.generate(regressors)
    # Smoothing du scénario
    reg_pfc_smooth = smoothing(reg_pfc, sampling_period=sampling_period, rolling_window=6)
    # Calibration du modèle stochastique pour le résidu
    residual_model.calibration(train_data = spot / spot.groupby(spot.index.year).transform('mean') - reg_pfc_smooth)
    # Calibration du modèle de markov pour générer les scénarios des variables explicatives
    regressor_model.calibration(train_data = normalized_regressors)
    
    # Génération scénarios Markov
    hourly_timestamp = pd.date_range(start=date(eval_years[0], 1, 1), end=date(eval_years[-1]+1, 1, 1), freq='h', inclusive='left')
    reg_scenarios, reg_names = regressor_model.generate(timestamp=hourly_timestamp, nscenario=nscenario)
    res_scenarios, res_name = residual_model.generate(timestamp=hourly_timestamp, nscenario=nscenario)

    if save_files:
        output_path = os.path.join(output_path, f"{date.today()}_{name}")
        os.makedirs(output_path, exist_ok=True)
        with open(os.path.join(output_path, 'inputs_calibration.json'), 'w', encoding='utf-8') as f:
            json.dump(inputs, f, ensure_ascii=False, indent=4)
        regression_model.save(dir=output_path)
        os.makedirs(os.path.join(output_path, 'markov'), exist_ok=True)
        np.save(os.path.join(output_path, 'markov', 'regressor_scenarios.npy'), reg_scenarios)
        np.save(os.path.join(output_path, 'markov', 'residual_scenarios.npy'), res_scenarios)
        names = {'residual' : res_name,
                'regressors' : reg_names}
        with open(os.path.join(output_path, 'markov', 'names.json'), 'w') as f:
            json.dump(names, f, indent=4)


def generate_normalized_pfc(calibration_path, scenario_file, pv_scenario_id, eol_scenario_id, load_scenario_id, generation_id=None, sampling_period='W', save_files=False):
    inputs = locals()

    # Chargement des données de calibration
    with open(os.path.join(calibration_path, 'inputs_calibration.json'), 'r') as f:
        eval_years = json.load(f)['eval_years']
    hourly_timestamp = pd.date_range(start=date(eval_years[0], 1, 1), end=date(eval_years[-1]+1, 1, 1), freq='h', inclusive='left')
    regression_model = HourlyReg()
    regression_model.load(calibration_path) 
    reg_scenarios = np.load(os.path.join(calibration_path, 'markov', 'regressor_scenarios.npy'))
    res_scenarios = np.load(os.path.join(calibration_path, 'markov', 'residual_scenarios.npy'))
    with open(os.path.join(calibration_path, 'markov', 'names.json'), 'r') as f:
        names = json.load(f)

    # Chargement des scénarios pour les variables explicatives
    prod_scenario = pd.read_excel(scenario_file, sheet_name='prod')
    load_scenario = pd.read_excel(scenario_file, sheet_name='conso')

    # Génération des regressors complets
    reg_coef = pd.DataFrame(index=hourly_timestamp, data={'Year' : hourly_timestamp.year})
    reg_coef['deltaTS'] = 1
    reg_coef = reg_coef.join(prod_scenario[prod_scenario.Scénario == pv_scenario_id].set_index('Année')['PV (GW)'].rename('pv_cf') * 1e3, on='Year')
    reg_coef = reg_coef.join(prod_scenario[prod_scenario.Scénario == eol_scenario_id].set_index('Année')[['Eolien onshore (GW)', 'Eolien offshore (GW)']].sum(axis=1).rename('eol_cf') * 1e3, on='Year')
    reg_coef = reg_coef.join(load_scenario[load_scenario.Scénario == load_scenario_id].set_index('Année')["Consommation (TWh)"].rename('load') * 1e6, on='Year')
    continuous_var = reg_scenarios.swapaxes(1, 0) * reg_coef[names['regressors']].to_numpy()
    nscenario, ntimestamp, nfeature = continuous_var.shape

    dummies_var = build_regressors(timestamp=hourly_timestamp)
    dummies_names = list(dummies_var.columns)
    ndummies = dummies_var.shape[1]
    dummies_var = np.broadcast_to(dummies_var.to_numpy(), (nscenario, ntimestamp, ndummies))

    regressors = np.concatenate((continuous_var, dummies_var), axis=2)
    regressors_df = pd.DataFrame(data=regressors.reshape(nscenario*ntimestamp, nfeature+ndummies), 
                                columns=names['regressors'] + dummies_names).rename(columns={'pv_cf' : 'pv', 'eol_cf' : 'eol'})
    
    # Génération de la PFC normalisée
    reg_component = regression_model.generate(regressors_df).to_numpy().reshape(nscenario, ntimestamp).swapaxes(1, 0)
    norm_PFC = pd.DataFrame(data=reg_component + res_scenarios, index=hourly_timestamp)
    norm_PFC = smoothing(norm_PFC, sampling_period=sampling_period)
    if save_files:
        output_path = os.path.join(calibration_path, generation_id)
        os.makedirs(output_path, exist_ok=True)
        norm_PFC.to_csv(os.path.join(output_path, f'normalized_pfc.csv'))
        with open(os.path.join(output_path, 'inputs_generation.json'), 'w', encoding='utf-8') as f:
            inputs.pop('save_files', None)
            json.dump(inputs, f, ensure_ascii=False, indent=4)
    
    return norm_PFC


def adjust_pfc(eex_path, historic_years, adjustment_date, norm_PFC=None, calibration_path=None, generation_id=None, sampling_period='W', save_files=False, return_quot=False):
    inputs = locals()

    # Chargement PFC normalisées
    if norm_PFC is None:
        with open(os.path.join(calibration_path, 'inputs_calibration.json'), 'r') as f:
            eval_years = json.load(f)['eval_years']
        generation_path = os.path.join(calibration_path, generation_id)
        norm_PFC = pd.read_csv(os.path.join(generation_path, f'normalized_pfc.csv'), index_col=0, parse_dates=True)
    else:
        eval_years = sorted(list(norm_PFC.index.year.unique()))
    hourly_timestamp = pd.date_range(start=date(eval_years[0], 1, 1), end=date(eval_years[-1]+1, 1, 1), freq='h', inclusive='left')

    # Chargement des cotations historiques
    eex_train = process_eex_df(bdd_to_dataframe(historic_years, eex_path, timestamp_index=False))
    M_Q_hist = get_hist_quotations(eex_train)

    # Ajustement des PFC aux cotations disponible
    adjustment_date = date.fromisoformat(adjustment_date)
    quotations = process_eex_df(pd.read_excel(os.path.join(eex_path, f'{adjustment_date.year}.xlsx')))
    M_adjusted, quot = get_adjusted_monthly_quotation(eex=quotations, trading_date=adjustment_date, years=eval_years, M_Q_hist=M_Q_hist, hourly_timestamp=hourly_timestamp, return_quot=return_quot)
    adj_PFC = adjust_PFC_to_month_quot(norm_PFC, M_adjusted)
    adj_PFC = smoothing(adj_PFC, sampling_period=sampling_period)
    if save_files:
        if generation_id is None:
            print('Files are not saved because no generation identifier has been given')
        else:
            output_path = os.path.join(generation_path, adjustment_date.isoformat())
            os.makedirs(output_path, exist_ok=True)
            adj_PFC.to_csv(os.path.join(output_path, f'adjusted_pfc.csv'))
            with open(os.path.join(output_path, f'inputs_adjustment.json'), 'w', encoding='utf-8') as f:
                inputs.pop('save_files', None)
                json.dump(inputs, f, ensure_ascii=False, indent=4)
    
    if return_quot:
        return adj_PFC, quot
    else:
        return adj_PFC, None