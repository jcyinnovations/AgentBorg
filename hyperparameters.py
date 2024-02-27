from typing import List, Tuple

class Params:
    min_expected_return: float = 0.03
    max_trailing_loss: float = 0.10
    CLASSES: Tuple[int, int, int] = (0, 1, 2)
    HOLD, BUY, SELL = CLASSES
    CLASS_LABELS: Tuple[str, str, str] = ('HOLD', 'BUY', 'SELL')
    
    # 30 minutes
    interval: int = 30
    # Intervals per day
    intervals_per_day: int = int(24 * 60/interval)
    
    # Important intervals
    max_holding_period: int = 1 # days
    max_look_back_period: int = 18 # days
    volatility_period: int = max_holding_period * 7 # days
    vols: dict = {'hv':volatility_period * intervals_per_day }

    # Moving averages
    smas_list: List[int] = [14, 50, 100, 350, 700]
    smas: dict = {
        's14':intervals_per_day*14
        , 's50':intervals_per_day*50
        , 's100':intervals_per_day*100
        , 's350':intervals_per_day*350
        , 's700':intervals_per_day*700
    }

    sma_cols = ["s{0}".format(s) for s in smas]
    
    # Fields in the inteference frame
    target_fields: List[str] = ['c', 's50', 's100', 's350', 's700', 'v', 'hv']
    target_fields_new: List[str] = ['c', 's14', 's50', 's100', 's350', 's700', 'v', 'hv']
    
    # SMA intervals in ticks
    #sma_intervals = [int(s * intervals_per_day) for s in smas]
    
    # Periods in 30-minute ticks
    holding_period_in_T: int = int(max_holding_period * intervals_per_day)
    look_back_period_in_T: int = int(max_look_back_period * intervals_per_day)
    volatility_period_in_T: int = volatility_period * intervals_per_day
    
    #Input to the inference engine
    obs_width: int = 24
    obs_height: int = int(look_back_period_in_T / obs_width)
    observation_shape: Tuple[int, int, int] = (obs_width, obs_height, len(target_fields))
    observation_size: int = look_back_period_in_T

    def __repr__(self):
        return f"Params(\n\tmax_holding_period: {Params.max_holding_period},\n" \
        f"\tmax_look_back_period: {Params.max_look_back_period}, \n\tvolatility_period: {Params.volatility_period} \n" \
        f"\tmin_expected_return: {Params.min_expected_return}, \n\tmax_trailing_loss: {Params.max_trailing_loss} \n" \
        f"\tsmas: {Params.smas} \n" \
        f"\ttarget_fields: {Params.target_fields} \n" \
        f"\tobservation_shape: {Params.observation_shape},\n\tinterval: {Params.interval}\n" \
        f"\tholding_period_in_T: {Params.holding_period_in_T},\n" \
        f"\tlook_back_period_in_T: {Params.look_back_period_in_T},\n" \
        f"\tvolatility_period_in_T: {Params.volatility_period_in_T},\n)"
