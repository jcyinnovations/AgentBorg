from db import DBManager, Asset, Tick, Timestep, ENVIRONMENT, MANAGER_ERROR
from bitfinex import load_ticks_to_now
from utils import process_ticks, compute_latest_stats, generate_timesteps_from_ticks, date_range, params, get_observation_v2, predict_via_serving

from typing import Tuple, List

from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
import time
import pendulum
import pandas as pd

import logging
import logging.config
import json
import click


def job_load_ticks(scheduler: BackgroundScheduler, manager: DBManager):
    '''
    Load ticks from Bitfinex and save to the database.
    Continue for a limited time (25 minutes (1500 seconds) maximum)
    '''
    logging.info("\t\tjob_load_ticks()")
    # First get the latest saved tick timestamp
    msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
    tick_store: dict = {}
    latest_tick_ts: datetime = None
    latest_timestep_ts: datetime = None
    last_tick: Tick = None
    msg, latest_tick_ts, latest_timestep_ts = manager.get_latest_timestamp(Asset.btcusd)
    msg2, prev_tick = manager.get_last_tick(Asset.btcusd)
    if msg == MANAGER_ERROR.SUCCESS and msg2 == MANAGER_ERROR.SUCCESS:
        prev_tick_df = prev_tick.to_df() # Needed if bitfinex ticks are incomplete
        logging.info(f"Latest Tick: {latest_tick_ts}, Latest Timestep: {latest_timestep_ts}")
        start_date: int = latest_tick_ts.timestamp() * 1000
        end_date: int = pendulum.now().int_timestamp * 1000
        success, tick_store = load_ticks_to_now(
            start_date
            , end_date
            , symbol=Asset.btcusd.value
        )
        if success:
            logging.info(f"Processing {len(tick_store)} ticks")
            df_ticks = process_ticks(
                tick_store
                , asset=Asset.btcusd.value
                , prev_tick=prev_tick_df
            )
            logging.info(f"Persist ticks to database: {manager.engine}")
            count = df_ticks.to_sql(name='tick', con=manager.engine, if_exists='append')
            logging.info(f"{count} ticks saved")
            if count:
                # Only continue if ticks saved successfully
                scheduler.add_job(
                    job_generate_timesteps
                    , 'date'
                    , args=[scheduler, latest_timestep_ts, manager]
                    , next_run_time=datetime.now()
                )
            else:
                logging.info(f"Failed to save ticks for range: {start_date} - {end_date}. Stopping...")


def job_generate_timesteps(
    scheduler: BackgroundScheduler
    , latest_timestep_ts: datetime
    , manager: DBManager
):
    ticks: pd.DataFrame
    msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
    logging.info(f"Get ticks after last Timestep {latest_timestep_ts}")
    msg, ticks = manager.get_ticks_after_last_timestep(latest_timestep_ts, Asset.btcusd)
    if msg == MANAGER_ERROR.SUCCESS:
        logging.info("Get most recent saved Timesteps for stat generation")
        # 34,000 is enough to recalculate the longest moving average (700 days)
        msg, current_timesteps = manager.get_recent_timesteps(34000, Asset.btcusd)
        if msg == MANAGER_ERROR.SUCCESS:
            logging.info(f"Retrieved Timesteps from {current_timesteps.index[0]} to {current_timesteps.index[-1]}")
            new_timesteps = generate_timesteps_from_ticks(ticks)
            new_timesteps['asset'] = Asset.btcusd
            logging.info(f"Created {len(new_timesteps)} timesteps from {new_timesteps.index[0]} to {new_timesteps.index[-1]}")
            new_timesteps = compute_latest_stats(new_timesteps, current_timesteps, latest_timestep_ts)
            logging.info(f"Persist new Timesteps")
            count = new_timesteps[new_timesteps.index > latest_timestep_ts].to_sql(name='timestep', con=manager.engine, if_exists='append')
            '''
            if count:
                logging.info("Schedule inference job")
                scheduler.add_job(
                    job_run_inference
                    , 'date'
                    , args=[scheduler, manager]
                    , next_run_time=datetime.now()
                )
            '''
            if not(count):
                logging.error("Failed to persist new timesteps.")


def job_run_inference(
    scheduler: BackgroundScheduler
    , manager: DBManager
    , model_endpoint: str
    , max_gap: timedelta
):
    logging.info(f"\t\tINFERENCE: {datetime.now()}")
    msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
    timesteps: pd.DataFrame
    msg, timesteps = manager.get_recent_timesteps(params.observation_size)
    if msg == MANAGER_ERROR.SUCCESS:
        latest_timestep: datetime = timesteps.index[-1].to_pydatetime()
        current_time: datetime = manager.utc_convert(datetime.utcnow())

        if current_time - latest_timestep < timedelta(minutes=max_gap):
            observation = get_observation_v2(timesteps)
            prediction = predict_via_serving(observation, endpoint=model_endpoint)
            logging.info(f"\t\tTRADE: {current_time}, {prediction}")
            '''
            scheduler.add_job(
                job_execute_trade
                , 'date'
                , args=[scheduler]
                , next_run_time=datetime.now()
            )
            '''


#def job_execute_trade(scheduler: BackgroundScheduler):
#    logging.info(f"\t\tTRADE: {datetime.now()}")


@click.command()
@click.option('--env', default="PREPROD", show_default=True, help='Environment key (PREPROD/PROD)')
@click.option('--schedule', default="1,31", show_default=True, help='Cron schedule per-hourly intervals to run.')
@click.option('--model_endpoint', default="http://localhost:8501/v1/models/model:predict", show_default=True, help='Tensorflow Serving REST model endpoint')
@click.option('--max_gap', default=30, type=int, show_default=True, help='Maxium lag between latest timestep and running inference (minutes)')
@click.option('--dburl', help="Database connection string")
#@click.option('--config', default="./config/scheduler.json", type=click.File('r'), help='Environment')
def main(env, schedule, dburl, model_endpoint, max_gap):
    manager: DBManager = None
    if dburl:
        manager = DBManager(db_url=dburl)
    else:
        manager = DBManager(environment=ENVIRONMENT.PREPROD)
    scheduler = BackgroundScheduler()
    # Add job to run every hour at 1 and 31 minutes past the hour
    scheduler.add_job(
        job_load_ticks
        , 'cron'
        , args=[scheduler, manager]
        , minute=schedule
    )
    scheduler.add_job(
        job_run_inference
        , 'cron'
        , args=[scheduler, manager, model_endpoint, max_gap]
        , minute=",".join([str(int(i)+4) for i in "1,31".split(',')])
    )
    # original intervals: '0,5,10,15,20,25,30,35,40,45,50,55'
    scheduler.start()

    # To keep the script running
    try:
        logging.info("===========================")
        logging.info(" Bot Scheduler Startup...")
        logging.info("===========================")
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        logging.error("Received an interrupt. Exiting...")
        scheduler.shutdown()


if __name__ == "__main__":
    with open('./config/logging.json') as config_file:
        logging.config.dictConfig(json.load(config_file))
    main()
