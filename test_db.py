import pytest

from sqlmodel import Session, SQLModel, select, create_engine, inspect
from db import DBManager, ENVIRONMENT, MANAGER_ERROR, Trade, Account
from db import Tick, Timestep, Asset, TradeType, Position
from datetime import datetime, timedelta, timezone
from typing import List, Tuple


@pytest.fixture
def db_manager():
    return DBManager()

'''
@pytest.fixture
def db_engine(db_manager):
    return db_manager.engine
'''

@pytest.fixture
def db_manager_with_schema(db_manager: DBManager):
    db_manager.create_schema()
    return db_manager
    

# Fixture for session management
@pytest.fixture
def session(db_manager):
    _session: Session = db_manager.get_session()
    yield _session
    _session.rollback()
    _session.close()

# Utility function to create dummy data
@pytest.fixture
def mock_ticks_btc():
    count: int = 101
    base_date: datetime = datetime.now()
    last_date: datetime = base_date - timedelta(minutes=100)
    ticks = []
    for i in range(count):
        dte: datetime = base_date - timedelta(minutes=i)
        tick = Tick(
            date=dte
            , o=100 + i
            , h=105 + i
            , l=95 + i
            , c=102 + i
            , v=1000 + i
            , asset=Asset.btcusd
        )
        ticks.append(tick)
    #    session.add(tick)
    #session.commit()
    return ticks

@pytest.fixture
def mock_timestep_btc(session):
    count: int = 101
    base_date: datetime = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    last_date: datetime = base_date + timedelta(minutes=(count - 1)*30)
    for i in range(count):
        timestep = Timestep(
            date=base_date + timedelta(minutes=i*30)
            , c=1000 + i
            , v=10000 + i
            , hv=2000 + i
            , s14=3000 + i
            , s50=4000 + i
            , s100=5000 + i
            , s350=6000 + i
            , s700=7000 + i
            , asset=Asset.btcusd
            , delta=8000
        )
        session.add(timestep)
    session.commit()
    return base_date, last_date

@pytest.fixture
def mock_candles():
    return [[1705679820000, 41009, 40968, 41009, 40968, 0.4049706]
            ,[1705679760000, 41071, 41009, 41071, 40995, 1.37254331]
            ,[1705679700000, 41064, 41048, 41064, 41047, 0.25740038]
            ,[1705679640000, 41044, 41052, 41052, 41044, 0.25529023]
            ,[1705679580000, 41053, 41030, 41053, 41030, 0.05377572]
            ,[1705679520000, 41053, 41053, 41053, 41030, 0.32334592]
            ,[1705679460000, 41059, 41058, 41090, 41058, 2.31687874]
            ,[1705679400000, 41045, 41082, 41082, 41044, 0.35701237]
            ,[1705679340000, 41095, 41019, 41108, 41019, 0.57101723]
            ,[1705679280000, 40965, 41078, 41078, 40965, 0.80070515]
            ,[1705679220000, 40922, 40964, 40964, 40922, 0.70567857]
            ,[1705679160000, 40866, 40914, 40914, 40866, 0.12455686]
            ,[1705679100000, 40900, 40868, 40935, 40868, 0.77221408]
            ,[1705679040000, 40910, 40896, 40920, 40896, 0.13822000]
            ,[1705678980000, 40916, 40913, 40951, 40913, 0.34206313]
            ,[1705678920000, 40964, 40910, 40964, 40910, 0.43856894]
            ,[1705678860000, 40950, 40945, 40950, 40945, 0.28473413]
            ,[1705678800000, 40934, 40957, 40957, 40934, 0.00023724]
            ,[1705678740000, 40965, 40965, 40974, 40965, 0.78031502]
            ,[1705678680000, 40919, 40966, 40966, 40913, 1.12601667]
            ,[1705678620000, 40842, 40949, 40966, 40842, 1.90707109]
            ,[1705678560000, 40840, 40836, 40840, 40779, 1.93583000]
            ,[1705678500000, 40860, 40836, 40889, 40835, 0.98893872]
            ,[1705678440000, 40874, 40859, 40876, 40859, 0.14575114]
            ,[1705678380000, 40885, 40879, 40885, 40872, 0.72087403]
            ,[1705678320000, 40887, 40889, 40927, 40887, 0.32766439]
            ,[1705678260000, 40862, 40875, 40875, 40862, 0.00210000]
    ]
        
@pytest.fixture
def account_and_position(session):
    # Create and insert mock data for account and positions
    # Assuming Account and Position are your model classes and session is your database session
    ts1: datetime = datetime.strptime("2023-05-25 15:30:00", '%Y-%m-%d %H:%M:%S')
    ts2: datetime = datetime.strptime("2023-05-25 16:00:00", '%Y-%m-%d %H:%M:%S')
    position1 = Position(asset=Asset.btcusd, pnl=0, spent=1, trailing_loss=0, value=1)
    position2 = Position(asset=Asset.ethusd, pnl=0, spent=1, trailing_loss=0, value=1)
    account1 = Account(cash=1, asset_value=0.5, balance=1.5, pnl=0, date=ts1, positions=[position1])
    account2 = Account(cash=1, asset_value=0.5, balance=1.5, pnl=0, date=ts2, positions=[position2])
    session.add(account1)
    session.add(account2)
    #session.add(position)
    session.commit()
    session.refresh(account2)
    a = account2.positions
    return account2

@pytest.fixture
def mock_ticks_btc_committed(session):
    count: int = 101
    base_date: datetime = datetime.now()
    last_date: datetime = base_date - timedelta(minutes=100)
    ticks = []
    for i in range(count):
        dte: datetime = base_date - timedelta(minutes=i)
        tick = Tick(
            date=dte
            , ts=int(int(dte.timestamp())*1000)
            , o=100 + i
            , h=105 + i
            , l=95 + i
            , c=102 + i
            , v=1000 + i
            , asset=Asset.btcusd
        )
        ticks.append(tick)
    session.add_all(ticks)
    session.commit()
    return ticks


### Test Cases ###

def test_create_schema(db_manager):
    db_manager.create_schema()
    # Assert tables are created; this might require a more detailed inspection depending on your setup
    # Use inspector to check if tables exist
    inspector = inspect(db_manager.engine)
    
    # List of expected table names from our SQLModel classes
    expected_tables = [Tick.__tablename__, Timestep.__tablename__, Trade.__tablename__, Position.__tablename__]
    
    # Fetch the list of tables in the database
    tables_in_db = inspector.get_table_names()
    
    # Check if each expected table exists in the database
    for table_name in expected_tables:
        assert table_name in tables_in_db, f"Table '{table_name}' was not created in the database."

def test_add_trade(db_manager_with_schema):
    #trade = Trade(date=datetime.now(), move=TradeType.BUY, asset=Asset.btcusd, amount=0.5, pct_acct=10.0, price=30000.0)
    trade: Trade
    msg: MANAGER_ERROR
    msg, trade = db_manager_with_schema.add_trade(ts=datetime.now(), move=TradeType.BUY, asset=Asset.btcusd, amount=0.5, pct_acct=10.0, price=30000.0)
    assert trade.date is not None, "Trade not committed"
    assert msg == MANAGER_ERROR.SUCCESS, f"add_trade() failed: {msg}"

def test_duplicate_trade(db_manager_with_schema):
    #trade = Trade(date=datetime.now(), move=TradeType.SELL, asset=Asset.ETH, amount=1.0, pct_acct=20.0, price=2000.0)
    ts: datetime = datetime.now()
    msg: MANAGER_ERROR
    trade: Trade
    msg, trade = db_manager_with_schema.add_trade(ts=ts, move=TradeType.SELL, asset=Asset.ethusd, amount=1.0, pct_acct=20.0, price=2000.0)
    #with pytest.raises(Exception):
    msg, trade = db_manager_with_schema.add_trade(ts=ts, move=TradeType.SELL, asset=Asset.ethusd, amount=1.0, pct_acct=20.0, price=2000.0)
    assert msg in (MANAGER_ERROR.DUPLICATE, MANAGER_ERROR.ERROR), f"Duplicate commit should throw a DUPLICATE error: {msg}"

def test_get_latest_frame(db_manager_with_schema, mock_timestep_btc):
    first: datetime = mock_timestep_btc[0]
    last: datetime = mock_timestep_btc[1]
    frame_size = 10
    msg: MANAGER_ERROR
    frame: List[Timestep]
    msg, frame = db_manager_with_schema.get_latest_frame(Asset.btcusd, frame_size)
    assert msg == MANAGER_ERROR.SUCCESS, f"get_latest_frame() failed: {msg}"
    assert len(frame) == frame_size, f"Expected {frame_size} items. Got {len(frame)}"
    assert frame[-1].date == last, f"Expected date of last row to be {last}. Found {frame[-1].date}"

def test_append_latest_ticks(db_manager_with_schema, mock_ticks_btc):
    msg: MANAGER_ERROR
    msg = db_manager_with_schema.append_latest_ticks(mock_ticks_btc)
    assert msg == MANAGER_ERROR.SUCCESS, "Could not append_latest_ticks()"

def test_candles_to_ticks(db_manager_with_schema, mock_candles):
    ticks: List[Tick] = db_manager_with_schema.candles_to_ticks(Asset.btcusd, mock_candles)
    assert len(ticks) == len(mock_candles), f"Could not convert raw bitfinex candles"

def test_append_bitfinex_candles(db_manager_with_schema, mock_candles):
    msg: MANAGER_ERROR = db_manager_with_schema.append_bitfinex_candles(Asset.btcusd, mock_candles)
    assert msg == MANAGER_ERROR.SUCCESS, f"Could not commit raw bitfinex candles {msg}"

def test_get_account_and_position_success(db_manager_with_schema, account_and_position):
    # Assuming 'BTC' is the asset we have data for
    msg: MANAGER_ERROR
    account: Account
    msg, account = db_manager_with_schema.get_account_and_position()
    
    # Verify the account details
    assert msg == MANAGER_ERROR.SUCCESS, f"Failure message received {msg}"
    assert account.date == account_and_position.date, f"Incorrect record retrieved. Dates don't match for retrieved account {account} and source {account_and_position}"
    assert len(account.positions) == 1, f"Expected a single position. Found {len(account.positions)}"
    assert account.positions[0].asset == Asset.ethusd, "Expected ETH, found {account.positions[0].asset}"

def test_update_account_and_position(db_manager_with_schema, account_and_position):
    msg: MANAGER_ERROR
    account: Account
    position: Position = Position(
        asset=account_and_position.positions[0].asset
        , value=account_and_position.positions[0].value
        , spent=account_and_position.positions[0].spent
        , pnl=account_and_position.positions[0].pnl
        , trailing_loss=account_and_position.positions[0].trailing_loss
    )

    msg, account = db_manager_with_schema.update_account_and_position(
                        cash=account_and_position.cash * 10
                        , asset_value=account_and_position.asset_value * 10
                        , balance=account_and_position.balance * 10
                        , pnl=account_and_position.pnl + 0.05
                        , update_time=datetime.now()
                        , positions=[position]
                    )
    assert msg == MANAGER_ERROR.SUCCESS, f"Failure message received {msg}"
    assert account.date != account_and_position.date, f"Update failed. Dates still match"
    assert account.cash == account_and_position.cash * 10, f"Update failed. Cash not updated"

def test_get_latest_timestamp(db_manager_with_schema, mock_ticks_btc_committed, mock_timestep_btc):
    msg: MANAGER_ERROR
    result: Tuple[MANAGER_ERROR, datetime, datetime]
    latest_tick_ts = db_manager_with_schema.utc_convert(mock_ticks_btc_committed[0].date)
    result = db_manager_with_schema.get_latest_timestamp()
    latest_timestep_ts = db_manager_with_schema.utc_convert(mock_timestep_btc[-1])

    assert result[0] == MANAGER_ERROR.SUCCESS, "get_latest_timestamp() failed"
    assert result[1] == latest_tick_ts, f"Ticks don't match. Got: {result[1]}, Expected: {latest_tick_ts}."
    assert result[2] == latest_timestep_ts, f"Timestamps don't match. Got: {result[2]}, Expected: {latest_timestep_ts}"

def test_get_recent_timesteps(db_manager_with_schema, mock_timestep_btc):
    msg: MANAGER_ERROR
    timesteps = None
    msg, timesteps = db_manager_with_schema.get_recent_timesteps(101)
    assert msg == MANAGER_ERROR.SUCCESS, "Failed to recover recent timesteps"
    assert timesteps.index[0] == mock_timestep_btc[0], "Dates on first row don't match"
    assert timesteps.index[-1] == mock_timestep_btc[1], "Dates on last row don't match"
    