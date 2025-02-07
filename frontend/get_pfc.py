from vercast.pfc import generate_normalized_pfc, adjust_pfc


def adjusted_pfc_from_scenario(calibration_path, eex_path, historic_years, adjustment_date, scenario_file, pv_scenario_id, eol_scenario_id, load_scenario_id):
    norm_PFC = generate_normalized_pfc(calibration_path=calibration_path, 
                                       scenario_file=scenario_file, 
                                       pv_scenario_id=pv_scenario_id, 
                                       eol_scenario_id=eol_scenario_id, 
                                       load_scenario_id=load_scenario_id)
    adj_PFC = adjust_pfc(eex_path=eex_path, 
                         historic_years=historic_years, 
                         adjustment_date=adjustment_date, 
                         norm_PFC=norm_PFC)
    return adj_PFC
