import pandas as pd
import numpy as np
import glob
import datetime
from scipy.stats import t
from tqdm import tqdm 

import warnings  # Import the warnings module

def upload_files_csv(path):
    files_csv = glob.glob(path + '*.csv')
    data = {}

    for file in files_csv:
        file_name = file.split('/')[-1]
        data[file_name] = pd.read_csv(file)

    return data

def process_data(data, parameters, ws):
    rain_limit = parameters['rain'][0]
    min_temp = parameters['temp'][0]
    max_temp = parameters['temp'][1]

    weekly_results_list = []

    pbar = tqdm(data.items(), desc=f"Processing data {ws}")

    for file_name, df in data.items():
        weekly_data = []

        for index, row in df.iterrows():
            date = pd.to_datetime(f"{int(row['year'])}-{int(row['month'])}-{int(row['day'])}")
            week = date.week
            year = int(row['year'])
            rain = row['prec'] > rain_limit
            no_rain = row['prec'] <= rain_limit
            min_temp_limit = row['t_min'] < min_temp
            max_temp_limit = row['t_max'] > max_temp

            weekly_data.append({
                'weather_station': file_name,
                'year': year,
                'week': week,
                'rainy_days': int(rain),
                'dry_days': int(no_rain),
                'heat_days': int(max_temp_limit),
                'cold_days': int(min_temp_limit)
            })
        pbar.update(1)

        weekly_results_list.append(pd.DataFrame(weekly_data))

    weekly_results = pd.concat(weekly_results_list, ignore_index=True)
    weekly_results = weekly_results.groupby(['weather_station', 'year', 'week']).sum().reset_index()
    pbar.close()
    return weekly_results

def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)
    mean = np.mean(data)
    std_err = np.std(data, ddof=1) / np.sqrt(n)
    interval = t.interval(confidence, n - 1, loc=mean, scale=std_err)
    return interval

def variation_coeff(x):

    return np.std(x, ddof=1) / np.mean(x) * 100

def quantiles(q):
    return lambda x: x.quantile(q)

def statistics(weekly_results, variables, metrics, conf_level=0.95):
    rows = []
    for var in variables:
        var_metrics = {metric_name: metric_fn for metric_name, metric_fn in metrics.items()}

        sample_size = weekly_results.groupby(['year', 'week'])[var].count()

        var_metrics['conf_lower'] = lambda x: t.interval(conf_level, len(x)-1, loc=np.mean(x), scale=np.std(x, ddof=1) / np.sqrt(len(x)))[0]
        var_metrics['conf_upper'] = lambda x: t.interval(conf_level, len(x)-1, loc=np.mean(x), scale=np.std(x, ddof=1) / np.sqrt(len(x)))[1]

        aggregated_data = weekly_results.groupby(['year', 'week'])[var].agg(**var_metrics).reset_index()
        aggregated_data['measure'] = var
        rows.append(aggregated_data)

    statistical_results = pd.concat(rows, ignore_index=True)
    return statistical_results

def add_dates_week(statistical_results):
    statistical_results['start'] = statistical_results.apply(
        lambda row: datetime.datetime.strptime(f"{int(row['year'])}-{int(row['week'])}-1", "%Y-%W-%w"), axis=1)
    statistical_results['end'] = statistical_results['start'] + datetime.timedelta(days=6)
    statistical_results.drop(['year'], axis=1, inplace=True)
    return statistical_results

def rearrange_columns(statistical_results, column_order):
    return statistical_results[column_order]

def main(data, parameters, ws, crop, soil=None):

    print("weekly results for: ", ws)
    weekly_results = process_data(data, parameters, ws)

    variables = ['rainy_days', 'dry_days', 'cold_days', 'heat_days']

    metrics = {
        'avg': np.mean,
        'median': np.median,
        'min': np.min,
        'max': np.max,
        'quar_1': quantiles(0.25),
        'quar_2': quantiles(0.50),
        'quar_3': quantiles(0.75),
        'conf_lower': lambda x: calculate_confidence_interval(x)[0],
        'conf_upper': lambda x: calculate_confidence_interval(x)[1],
        'sd': np.std,
        'perc_5': quantiles(0.05),
        'perc_95': quantiles(0.95),
        'coef_var': variation_coeff
    }

    print("statistical results for: ", ws)
    statistical_results = statistics(weekly_results, variables, metrics)
    statistical_results['weather_station'] = ws
    statistical_results['cultivar'] = crop
    statistical_results = add_dates_week(statistical_results)

    #Acrónimos
    statistical_results['measure'] = statistical_results['measure'].replace({'rainy_days': 'ag_ndll', 'dry_days': 'ag_nds', 'heat_days': 'ag_ndc', 'cold_days': 'ag_ndf'})

    column_order = ['weather_station', 'cultivar', 'start', 'end', 'measure', 'week', 'avg', 'median', 'min', 'max', 'quar_1', 'quar_2', 'quar_3', 'conf_lower', 'conf_upper', 'sd', 'perc_5', 'perc_95', 'coef_var']
    statistical_results = rearrange_columns(statistical_results, column_order)

    return statistical_results

if __name__ == "__main__":
    main()