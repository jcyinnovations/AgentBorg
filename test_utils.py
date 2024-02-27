import pytest
import pandas as pd
from datetime import datetime, timezone
from utils import interval_infill, process_ticks, generate_timesteps_from_ticks, compute_latest_stats

@pytest.fixture
def array_of_ticks():
    return [
        [1705679280000, 40965, 41078, 41078, 40965, 0.80070515]
        ,[1705679340000, 41095, 41019, 41108, 41019, 0.57101723]
        ,[1705679400000, 41045, 41082, 41082, 41044, 0.35701237]
        ,[1705679460000, 41059, 41058, 41090, 41058, 2.31687874]
        ,[1705679520000, 41053, 41053, 41053, 41030, 0.32334592]
        ,[1705679580000, 41053, 41030, 41053, 41030, 0.05377572]
        ,[1705679640000, 41044, 41052, 41052, 41044, 0.25529023]
        ,[1705679700000, 41064, 41048, 41064, 41047, 0.25740038]
        ,[1705679760000, 41071, 41009, 41071, 40995, 1.37254331]
        ,[1705679820000, 41009, 40968, 41009, 40968, 0.4049706]
    ]

@pytest.fixture
def ticks():
    df = pd.DataFrame(
        [
        [1705679280000, 40965, 41078, 41078, 40965, 0.80070515]
        ,[1705679340000, 41095, 41019, 41108, 41019, 0.57101723]
        ,[1705679400000, 41045, 41082, 41082, 41044, 0.35701237]
        ,[1705679460000, 41059, 41058, 41090, 41058, 2.31687874]
        ,[1705679520000, 41053, 41053, 41053, 41030, 0.32334592]
        ,[1705679580000, 41053, 41030, 41053, 41030, 0.05377572]
        ,[1705679640000, 41044, 41052, 41052, 41044, 0.25529023]
        ,[1705679700000, 41064, 41048, 41064, 41047, 0.25740038]
        ,[1705679760000, 41071, 41009, 41071, 40995, 1.37254331]
        ,[1705679820000, 41009, 40968, 41009, 40968, 0.4049706]
        ]
        , columns=['ts','o','c','h','l','v']
    )
    df['date'] = df['ts'].apply(lambda ts: datetime.fromtimestamp(ts/1000, timezone.utc))
    df = df.reset_index(drop=True)
    df = df.set_index('date')
    return df


@pytest.fixture
def ticks_with_stats():
    df = pd.DataFrame(
        [
        [1705679280000, 40965, 41078, 41078, 40965, 0.80070515]
        ,[1705679340000, 41095, 41019, 41108, 41019, 0.57101723]
        ,[1705679400000, 41045, 41082, 41082, 41044, 0.35701237]
        ,[1705679460000, 41059, 41058, 41090, 41058, 2.31687874]
        ,[1705679520000, 41053, 41053, 41053, 41030, 0.32334592]
        ,[1705679580000, 41053, 41030, 41053, 41030, 0.05377572]
        ,[1705679640000, 41044, 41052, 41052, 41044, 0.25529023]
        ,[1705679700000, 41064, 41048, 41064, 41047, 0.25740038]
        ,[1705679760000, 41071, 41009, 41071, 40995, 1.37254331]
        ,[1705679820000, 41009, 40968, 41009, 40968, 0.4049706]
        ]
        , columns=['ts','o','c','h','l','v']
    )
    df['date'] = df['ts'].apply(lambda ts: datetime.fromtimestamp(ts/1000, timezone.utc))
    df = df.reset_index(drop=True)
    df = df.set_index('date')
    df['hv'] = df['c'].rolling('3T').std()
    df['s3'] = df['c'].rolling('3T').mean()
    return df


@pytest.fixture
def ticks_with_infill():
    df = pd.DataFrame(
        [
        [1705679280000, 40965, 41078, 41078, 40965, 0.80070515]
        ,[1705679400000, 41045, 41082, 41082, 41044, 0.35701237]
        ,[1705679460000, 41059, 41058, 41090, 41058, 2.31687874]
        ,[1705679520000, 41053, 41053, 41053, 41030, 0.32334592]
        ,[1705679580000, 41053, 41030, 41053, 41030, 0.05377572]
        ,[1705679640000, 41044, 41052, 41052, 41044, 0.25529023]
        ,[1705679700000, 41064, 41048, 41064, 41047, 0.25740038]
        ,[1705679760000, 41071, 41009, 41071, 40995, 1.37254331]
        ,[1705679820000, 41009, 40968, 41009, 40968, 0.4049706]
       ]
       , columns=['ts','o','c','h','l','v']
    )

@pytest.fixture
def ticks_with_gaps():
    df = pd.DataFrame(
        [
        [1705679280000, 40965, 41078, 41078, 40965, 0.80070515]
        ,[1705679580000, 41053, 41030, 41053, 41030, 0.05377572]
        ,[1705679640000, 41044, 41052, 41052, 41044, 0.25529023]
        ,[1705679760000, 41071, 41009, 41071, 40995, 1.37254331]
        ,[1705679820000, 41009, 40968, 41009, 40968, 0.4049706]
        ]
        , columns=['ts','o','c','h','l','v']
    )
    df['date'] = df['ts'].apply(lambda ts: datetime.fromtimestamp(ts/1000, timezone.utc))
    df = df.reset_index(drop=True)
    df = df.set_index('date')
    return df


def test_interval_infill(ticks, ticks_with_gaps):
    df = interval_infill(ticks_with_gaps)
    assert len(df) == len(ticks), f"Expected post-infill dataframe size of {len(ticks)}. Got {len(df)} instead"
    assert (df.index == ticks.index).all(), f"Date sequence doesn't match {df}"
    assert (df['o'][1:4] == [40982.6, 41000.2, 41017.8]).all(), f"Infill incorrect {df['o'].to_numpy()}"


def test_process_ticks(array_of_ticks, ticks):
    df = process_ticks(array_of_ticks, asset='btcusd')
    ticks['asset'] = 'btcusd'
    assert (df.all() == ticks[df.columns.to_numpy()].all()).all(), f"Tick conversion to DataFrame failed {df}"


def test_generate_timesteps_from_ticks(ticks):
    df = generate_timesteps_from_ticks(ticks, interval='5T')
    expected = pd.DataFrame(
        [
            [40965, 41108, 40965, 41019, 1.37172238]
            , [41045, 41090, 41030, 41052, 3.30630298]
            , [41064, 41071, 40968, 40968, 2.03491429]
        ]
        , columns=['o', 'h', 'l', 'c', 'v']
    )
    assert len(df) == len(expected), f"Incorrect length for source: {len(df)}, expected: {len(expected)}"
    assert (df.all() == expected.all()).all(), f"Result {df}, doesn't match expected: {expected}"


def test_compute_latest_stats(ticks_with_stats, ticks):
    current_timesteps = ticks_with_stats[:8].copy()
    new_timesteps = ticks[8:].copy()

    df_update = compute_latest_stats(
        new_timesteps=new_timesteps
        , current_timesteps=current_timesteps
        , sma_list={'s3': 3}
        , vol_list={'hv': 3}
    )
    msg = f"Streaming stats incorrect. Expected {ticks_with_stats[8:]}, but got {df_update}"
    assert (ticks_with_stats[8:].all() == df_update.all()).all(), msg
