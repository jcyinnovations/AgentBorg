from sqlmodel import SQLModel, Field, create_engine, select, Session, Column, Enum, func, Relationship
from sqlalchemy.exc import IntegrityError
from typing import Optional, List, Tuple, Any 
from datetime import datetime, timezone
import pandas as pd

import enum
import pendulum
import logging

DB_CONNECT_URL = {
    "TEST": "sqlite:///TEST.db"
    , "UNIT": "sqlite:///:memory:"
    , "PREPROD": "sqlite:///PREPROD_20240208.db"
    , "PROD": "sqlite:///PRODUCTION.db"
}

class MANAGER_ERROR(int, enum.Enum):
    SUCCESS = 0
    DUPLICATE = 1
    BITFINEX_ERROR = 2
    TIMEOUT = 3
    ERROR = 999

class ENVIRONMENT(str, enum.Enum):
    TEST = "TEST"
    UNIT = "UNIT"
    PREPROD = "PREPROD"
    PROD = "PROD"
    PROD_V2 = "PROD_V2"

class TradeType(int, enum.Enum):
    HOLD = 0
    BUY = 1
    SELL = 2

class Asset(str, enum.Enum):
    btcusd = "btcusd"
    ethusd = "ethusd"

class Tick(SQLModel, table=True):
    date: datetime = Field(primary_key=True, nullable=False)
    o: float = Field(nullable=False)
    h: float = Field(nullable=False)
    l: float = Field(nullable=False)
    c: float = Field(nullable=False)
    v: float = Field(nullable=False)
    asset: Asset 
    
    def to_df(self):
        data = self.model_dump()
        df = pd.DataFrame({k: [data[k]] for k in data})
        df['date'] = df['date'].apply(lambda dte: pendulum.timezone('utc').convert(dte))
        df.reset_index(drop=True, inplace=True)
        df.set_index('date', inplace=True)
        return df

    def to_array(self):
        return list(self.model_dump().values)

class Timestep(SQLModel, table=True):
    date: datetime = Field(primary_key=True, nullable=False)
    c: float = Field(nullable=False)
    v: float = Field(nullable=False)
    hv: float = Field(nullable=False)
    s14: float = Field(nullable=False)
    s50: float = Field(nullable=False)
    s100: float = Field(nullable=False)
    s350: float = Field(nullable=False)
    s700: float = Field(nullable=False)
    delta: float = Field(nullable=False)
    asset: Asset 

class Trade(SQLModel, table=True):
    date: datetime = Field(primary_key=True, nullable=False)
    move: TradeType 
    asset: Asset
    amount: float = Field(nullable=False)
    pct_acct: float = Field(nullable=False)
    price: float = Field(nullable=False)

class Position(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    spent: float = Field(nullable=False)
    value: float = Field(nullable=False)
    pnl: float = Field(nullable=False)
    trailing_loss: float = Field(nullable=False)
    asset: Asset
    account_id: Optional[datetime] = Field(default=None, foreign_key="account.date")
    account: "Account" = Relationship(back_populates="positions")

class Account(SQLModel, table=True):
    date: datetime = Field(primary_key=True, nullable=False)
    cash: float = Field(nullable=False)
    asset_value: float = Field(nullable=False)
    balance: float = Field(nullable=False)
    pnl: float = Field(nullable=False)
    positions: List["Position"] = Relationship(back_populates="account")

class DBManager():

    def __init__(
        self
        , db_url: str=None
        , environment: ENVIRONMENT=ENVIRONMENT.UNIT
        , new_db=False
    ) -> None:
        if db_url:
            self.engine = create_engine(db_url)
            self.environment = db_url
        else:
            self.engine = create_engine(DB_CONNECT_URL[environment.value])
            self.environment = environment
        self.utc = pendulum.timezone('utc')
        if new_db:
            self.create_schema()
        self.session = None

    def utc_convert(self, ts):
        return self.utc.convert(ts)

    def get_latest_timestamp(
        self
        , asset: Asset=Asset.btcusd
    ) -> Tuple[MANAGER_ERROR, datetime, datetime]:
        '''
        Get the timestamp of the most recent tick update
        and timestep update
        '''
        result: Tuple[MANAGER_ERROR, datetime, datetime] = None, None, None
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        try:
            with self.get_session() as session:
                statement = select(func.max(Tick.date)).where(Tick.asset == asset) #.order_by(Ticks.date.desc()).limit(1)
                last_tick: datetime = session.exec(statement=statement).first()
                statement = select(func.max(Timestep.date)).where(Timestep.asset == asset) #.order_by(Timestep.date.desc()).limit(1)
                last_timestep: datetime = session.exec(statement=statement).first()
                result = msg, self.utc_convert(last_tick), self.utc_convert(last_timestep)
                session.close()
        except Exception as e:
            result = MANAGER_ERROR.ERROR, None, None
            logging.error(f"Failed get_latest_timestamp(): {e}")
            logging.exception(e)
        return result

    def add_trade(
        self
        , ts: datetime
        , move: TradeType
        , asset: Asset
        , amount: float
        , pct_acct: float
        , price: float
    ) -> Tuple[MANAGER_ERROR, Trade]:
        '''
        Update trade log
        '''
        trade: Trade = None
        if ts is None:
            ts = datetime.now()
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        try:
            with self.get_session() as session:
                trade = Trade(date=ts, move=move, asset=asset, amount=amount, pct_acct=pct_acct, price=price)
                session.add(trade)
                session.commit()
                session.refresh(trade)
                session.close()
        except Exception as e:
            msg = MANAGER_ERROR.ERROR
            logging.error(f"Failed enter_trade(): {e}")
            logging.exception(e)
        return msg, trade

    def create_account(
        self
        , update_time: datetime
        , cash: float
        , asset_value: float
        , balance: float
        , pnl: float
    ) -> Tuple[MANAGER_ERROR, Account]:
        '''
        Starting out with a fresh db, create a new account
        '''
        account: Account = None
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        try:
            with self.get_session() as session:
                account = Account(
                    date=update_time
                    , cash=cash
                    , asset_value=asset_value
                    , balance=balance
                    , pnl=pnl
                )
                session.add(account)
                session.commit()
                session.refresh(account)
                session.close()
        except Exception as e:
            msg = MANAGER_ERROR.ERROR
            logging.error(f"Failed create_account(): {e}")
            logging.exception(e)
        return msg, account

    def update_account_and_position(
        self
        , cash: float
        , asset_value: float
        , balance: float
        , pnl: float
        , update_time: datetime
        , positions: List[Position]=None
    ) -> Tuple[MANAGER_ERROR, Account]:
        '''
        Based on action taken, update last account state
        and append to db as new account and position state
        The default update_time is `now` but can be changed
        to match, e.g the exchange timestamp

        account: dictionary dump of the updated Account
        positions: list of new positions as dictionary
        update_time: timestamp index of the new update

        Dictionaries taken by dumping and modifying each model 
        using `model_dump()`
        '''
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        if not update_time:
            update_time = datetime.now()
        
        msg: MANAGER_ERROR
        account: Account

        msg, account = self.create_account(
            update_time
            , cash
            , asset_value
            , balance
            , pnl
        )

        if msg == MANAGER_ERROR.SUCCESS:
            if positions:
                try:
                    with self.get_session() as session:
                        session.add(account)
                        account.positions = positions
                        session.commit()
                        session.refresh(account)
                        a = account.positions
                        session.close()
                except Exception as e:
                    msg = MANAGER_ERROR.ERROR
                    logging.error(f"Failed update_account_and_position() {e}")
                    logging.exception(e)
        return msg, account


    def get_account_and_position(
        self
    ) -> Tuple[MANAGER_ERROR, Account]:
        ''' 
        Get current account balance and position
        '''
        account: Account = None
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        try:
            with self.get_session() as session:
                #statement = select(Account, Position).join(Account).order_by(Account.date.desc()).limit(2)
                statement = select(Account).order_by(Account.date.desc()).limit(1)
                account = session.exec(statement=statement).first()
                if account:
                    # Fetch associated positions
                    a = account.positions
                session.close()
        except Exception as e:
            msg = MANAGER_ERROR.ERROR
            logging.error(f"Failed get_account_and_position() {e}")
            logging.exception(e)
        return msg, account

    def get_latest_frame(
        self
        , asset: Asset = Asset.btcusd
        , frame_length: int = 100
    ) -> Tuple[MANAGER_ERROR, List[Timestep]]:
        '''
        Load the latest inference dataframe and convert to 
        an inference object
        '''
        frame: List[Timestep] = None
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        try:
            with self.get_session() as session:
                statement = select(Timestep).where(Timestep.asset == asset).order_by(Timestep.date.desc()).limit(frame_length)
                frame = session.exec(statement=statement).all()
                if frame:
                    frame.reverse()
                session.close()
        except Exception as e:
            msg = MANAGER_ERROR.ERROR
            logging.error("Failed get_latest_frame()")
            logging.exception(e)
        return msg, frame

    def append_latest_ticks(
        self
        , ticks: List[Tick]
    ) -> MANAGER_ERROR:
        '''
        Append most recent ticks
        '''
        msg = MANAGER_ERROR.SUCCESS
        try:
            with self.get_session() as session:
                session.add_all(ticks)
                session.commit()
                session.close()
        except IntegrityError as ie:
            msg = MANAGER_ERROR.Duplicate
            logging.error(f"Tried to commit a duplicate record. Check timestamps. {ie._message}, {ie._sql_message}")
            logging.debug(ie)
        except Exception as e:
            msg = MANAGER_ERROR.ERROR
            logging.error(f"Error: {e}")
            logging.exception(e)
        return msg

    def candles_to_ticks(
        self
        , asset: Asset
        , candles: List[Any]
    ) -> Tick:
        '''
        Candle: time in milliseconds, o, c, h, l, v
        '''
        ticks: List[Tick] = []
        for candle in candles:
            tick: Tick = Tick(
                date=datetime.utcfromtimestamp(candle[0]/1000)
                , ts=candle[0]
                , o=candle[1]
                , c=candle[2]
                , h=candle[3]
                , l=candle[4]
                , v=candle[5]
                , asset=asset
            )
            ticks.append(tick)
        return ticks

    def append_bitfinex_candles(
        self
        , asset: Asset
        , candles: List[Any]
    ) -> MANAGER_ERROR:
        '''
        Append most recent ticks
        '''
        msg = MANAGER_ERROR.SUCCESS
        try:
            with self.get_session() as session:
                session.add_all(self.candles_to_ticks(asset, candles))
                session.commit()
                session.close()
        except IntegrityError as ie:
            msg = MANAGER_ERROR.DUPLICATE
            logging.error(f"Tried to commit a duplicate record. Check timestamps. {ie._message}, {ie._sql_message}")
            logging.debug(ie)
        except Exception as e:
            msg = MANAGER_ERROR.ERROR
            logging.error(f"Error: {e}")
            logging.exception(e)
        return msg

    def get_ticks_after_last_timestep(
        self
        , latest_timestep_ts: datetime
        , asset: Asset = Asset.btcusd
    ) -> Tuple[MANAGER_ERROR, pd.DataFrame]:
        '''
        Get ticks after the last timestep
        '''
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        df: pd.DataFrame = None

        try:
            with self.get_session() as session:
                statement = select(Tick).where(Tick.date > latest_timestep_ts).where(Tick.asset == asset).order_by(Tick.date.asc())
                #ticks = session.exec(statement).all()
                df = pd.read_sql(statement, self.engine)
                df['date'] = df['date'].apply(lambda dte: pendulum.timezone('utc').convert(dte) )
                df.reset_index(drop=True, inplace=True)
                df.set_index('date', inplace=True)
                session.close()
        except Exception:
            msg = MANAGER_ERROR.ERROR
            logging.exception(f"Failed to get latest ticks after {latest_timestep_ts}")
        return msg, df

    def get_recent_timesteps(
        self
        , frame_length: int = 34000
        , asset: Asset=Asset.btcusd
    ) -> Tuple[MANAGER_ERROR, pd.DataFrame]:
        '''
        Get frame_length latest Timesteps
        '''
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        df: pd.DataFrame = None

        try:
            with self.get_session() as session:
                statement = select(Timestep).where(Timestep.asset == asset).order_by(Timestep.date.desc()).limit(frame_length)
                df = pd.read_sql(statement, self.engine)
                df['date'] = df['date'].apply(lambda dte: pendulum.timezone('utc').convert(dte) )
                df.reset_index(drop=True, inplace=True)
                df.set_index('date', inplace=True)
                df.sort_index(inplace=True)
                session.close()
        except Exception:
            msg = MANAGER_ERROR.ERROR
            logging.exception(f"Failed to get latest {frame_length} timesteps")
        return msg, df

    def get_last_tick(self, asset: Asset = Asset.btcusd) -> Tuple[MANAGER_ERROR, Tick]:
        msg: MANAGER_ERROR = MANAGER_ERROR.SUCCESS
        tick: Tick = None
        try:
            with self.get_session() as session:
                statement = select(Tick).where(Tick.asset == asset).order_by(Tick.date.desc()).limit(1)
                tick = session.exec(statement=statement).first()
        except Exception:
            msg = MANAGER_ERROR.ERROR
            logging.exception(f"Failed to get last tick for {asset}")
        return msg, tick


    def create_schema(self):
        '''
        Create tables if they do not exist
        '''
        #SQLModel.metadata.drop_all()
        SQLModel.metadata.create_all(self.engine)

    def get_session(self) -> Session:
        '''
        Create a session to the chosen database if none exists or closed
        '''
        #if self.session is None or self.session.connection().closed:
        #    self.session = Session(self.engine)
        return Session(self.engine)
