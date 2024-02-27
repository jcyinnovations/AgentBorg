import pandas as pd
import numpy as np
import statistics
import logging
from sklearn import preprocessing
import json
import requests

from datetime import datetime, timedelta, timezone
from typing import Tuple, List, Any

from hyperparameters import Params

params: Params = Params()


def interval_infill(df):
    '''
    Fill gaps in the time series with interpolation
    Expect columns: ['ts','o','c','h','l','v']
    '''
    df['interval'] = df.index.to_series().diff().dt.seconds/60
    jump_intervals = df[df.interval > 1]
    merged_df = [df]
    for index, row in jump_intervals.iterrows():
        # create a new dataframe with missing timestamps
        count = int(row['interval'] - 1)
        #fill = [np.nan for i in range(count)]
        new_df = pd.DataFrame({
            'date': pd.date_range(index - pd.Timedelta(row['interval']-1, 'min'), index - pd.Timedelta(1, 'min'), freq='1min')
            , 'o': np.nan
            , 'h': np.nan
            , 'l': np.nan
            , 'c': np.nan
            , 'v': np.nan
        })
        new_df.set_index('date', inplace=True)
        merged_df.append(new_df)  
    merged_df = pd.concat(merged_df)
    merged_df.sort_index(inplace=True)
    #logging.debug(f"Merged_df before infill {merged_df}")
    merged_df.interpolate(method='linear', inplace=True)
    merged_df.drop(columns=['interval'], inplace=True)
    return merged_df


def process_ticks(
    ticks: List[Any]
    , asset
    , prev_tick
) -> pd.DataFrame:
    '''
    Convert tick array to dataframe and infill where necessary.
    Generate 30-minute dataframe and return both
    Save to database if engine specified
    '''
    logging.info(f"Convert tick array to DataFrame. {ticks[:5]} - {ticks[-5:]}")
    df = pd.DataFrame(ticks, columns=['ts','o','c','h','l','v'])
    df['date'] = df['ts'].apply(lambda ts: datetime.fromtimestamp(ts/1000, timezone.utc))
    df.reset_index(drop=True, inplace=True)
    df.set_index('date', inplace=True)
    #pickle_file = f"./process_ticks_before_infill_{df['ts'].iloc[0]}-{df['ts'].iloc[-1]}.p"
    logging.info("Run infill...")
    df.drop(columns=['ts'], inplace=True)
    df.sort_index(inplace=True) # Ticks from Bitfinex are reverse order. Sort by date first
    df = pd.concat([prev_tick, df]) # Preprend previous tick to ensure no gap to it
    df = interval_infill(df)
    df.drop(df.index[0], inplace=True) # Remove the prepended previous tick
    df['asset'] = asset
    #logging.info(f"Infilled: {df}")
    return df


def generate_timesteps_from_ticks(df: pd.DataFrame, interval: str = '30T') -> pd.DataFrame:
    logging.info(f"Generate timesteps for interval {interval} from {len(df)} ticks")
    df_interval = df.resample(interval).agg({
        'o': 'first'
        , 'h': 'max'
        , 'l': 'min'
        , 'c': 'last'
        , 'v': 'sum'
        , 'asset': 'last'
    })
    return df_interval


#max_holding_period = 1  # days
#holding_period_in_T = int(max_holding_period * 24 * 2) # 30-minute ticks so x2
#volatility_period = holding_period_in_T * 7
#smas = {'s14':48*14, 's50':48*50, 's100':48*100, 's350':48*350, 's700':48*700}
#vols = {'hv':volatility_period }

#params.smas

def streaming_stats(
    df: pd.DataFrame
    , c_n: float
    , sma_list: dict=params.smas
    , vol_list: dict=params.vols
) -> dict:
    '''
        df - main dataframe with current data
        c_n - most recent close value
        sma_list - dict of moving averages durations
        vol_list - dict of volatility durations
    '''
    tip_idx = df.index[-1]
    results = {}
    
    for sma_key in sma_list.keys():
        mu_0 = df[sma_key].iloc[-1]
        N = sma_list[sma_key]
        c_0 = df['c'].iloc[-N]
        mu_n = mu_0 - c_0/N + c_n/N
        results[sma_key] = mu_n
    for vol_key in vol_list.keys():
        N = vol_list[vol_key]
        sigma_n = statistics.stdev(df['c'][1-N:].to_list() + [c_n])
        results[vol_key] = sigma_n
    c_prev = df['c'][tip_idx]
    results['delta'] = (c_n - c_prev)/c_prev
    return results


def compute_latest_stats(
    new_timesteps: pd.DataFrame
    , current_timesteps: pd.DataFrame
    , latest_timestep_ts: datetime
    , sma_list: dict=params.smas
    , vol_list: dict=params.vols
) -> pd.DataFrame:
    for row in new_timesteps[new_timesteps.index > latest_timestep_ts].iterrows():
        row_dict = row[1].to_dict()
        c_n = row[1]['c']
        new_stats = streaming_stats(
            current_timesteps
            , c_n
            , sma_list=sma_list
            , vol_list=vol_list
        )
        row_dict.update(new_stats)
        #end_idx = len(current_timesteps)
        current_timesteps.loc[row[0]] = row_dict
    # Return the updated rows only
    return current_timesteps[current_timesteps.index >= new_timesteps.index[0]]


def date_range(start, end, step):
    """
    Generator that yields a tuple of datetime-like objects
    which are `step` apart until the final `end` date
    is reached.
    """
    curr = start
    while curr < end:
        next_ = curr+step
        # next step is bigger than end date
        # yield last (shorter) step until final date
        if next_ > end:
            yield curr, end
            break
        else:
            yield curr, next_
            curr = next_ + 60*1000 # Next start is current end + 1-minute


def group_normalize(df):
    '''
        MinMax normalize a set of Pandas dataframe columns together
        Used to co-normalize price and moving averages
    '''
    # fit scaler across all columns
    scaler = preprocessing.PowerTransformer().fit(df.values.reshape(-1,1))
    # scale each column with new scaler
    scaled_data = {}
    for c in df.columns:
        scaled_data[c] = scaler.transform(df[c].values.reshape(-1,1))[:,0]
    return pd.DataFrame(scaled_data)


def column_normalize(column, scaler=None):
    '''
        Normalize a single column with its own scaler op
    '''
    scaled_column = column
    if scaler:
        scaled_column = scaler.transform(column.values.reshape(-1,1))[:,0]
    else:
        scaled_column = preprocessing.PowerTransformer().fit_transform(column.values.reshape(-1,1))[:,0]

    return scaled_column


def get_observation_v2(base):
    '''
    Get an observation starting at time T, window-normalize volume, reshape into 3D array
    Save the current delta for recalculation of equity if required (assets > 0)
    N = lookback_period_in_T
    '''
    base = base[params.target_fields]
    v_idx = params.target_fields.index('v')
    # scale volume
    scaled_v = column_normalize(base['v'])
    # scale price and volatility fields
    tf = params.target_fields.copy()
    tf.remove('v')
    base = group_normalize(base[tf])
    base['v'] = scaled_v
    base = base.to_numpy()
    # Insert time slices of look-back and future trading horizon
    return base.reshape(params.observation_shape).astype('float32')


def observation_json(observation):
    batched_img = np.array([observation])
    data = json.dumps(
        {
            "signature_name": "serving_default",
            "instances": batched_img.tolist(),
        }
    )

def predict_via_serving(observation, endpoint):
    json_data = observation_json(observation)

    json_response = requests.post(endpoint, data=json_data)
    response = json.loads(json_response.text)
    logging.debug(response)
    return response["predictions"]
