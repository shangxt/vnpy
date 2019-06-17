from datetime import datetime, timedelta
from typing import List

from rqdatac import init as rqdata_init
from rqdatac.services.basic import all_instruments as rqdata_all_instruments
from rqdatac.services.get_price import get_price as rqdata_get_price

from .setting import SETTINGS
from .constant import Exchange, Interval
from .object import BarData, TickData, HistoryRequest


INTERVAL_VT2RQ = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "60m",
    Interval.DAILY: "1d",
}

INTERVAL_ADJUSTMENT_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()         # no need to adjust for daily bar
}


class RqdataClient:
    """
    Client for querying history data from RQData.
    """

    def __init__(self):
        """"""
        self.username = SETTINGS["rqdata.username"]
        self.password = SETTINGS["rqdata.password"]

        self.inited = False
        self.symbols = set()

    def init(self):
        """"""
        if self.inited:
            return True

        # if not self.username or not self.password:
        #             return False

        rqdata_init(self.username, self.password,
                    ('rqdatad-pro.ricequant.com', 16011))

        try:
            df = rqdata_all_instruments(date=datetime.now())
            for ix, row in df.iterrows():
                self.symbols.add(row['order_book_id'])
        except RuntimeError:
            return False

        self.inited = True
        return True

    def to_rq_symbol(self, symbol: str, exchange: Exchange):
        """
        CZCE product of RQData has symbol like "TA1905" while
        vt symbol is "TA905.CZCE" so need to add "1" in symbol.
        """
        if exchange in [Exchange.SSE, Exchange.SZSE]:
            if exchange == Exchange.SSE:
                rq_symbol = f"{symbol}.XSHG"
            else:
                rq_symbol = f"{symbol}.XSHE"
        else:
            if exchange is not Exchange.CZCE:
                return symbol.upper()

            for count, word in enumerate(symbol):
                if word.isdigit():
                    break

            # noinspection PyUnboundLocalVariable
            product = symbol[:count]
            year = symbol[count]
            month = symbol[count + 1:]

            if year == "9":
                year = "1" + year
            else:
                year = "2" + year

            rq_symbol = f"{product}{year}{month}".upper()

        return rq_symbol

    def query_history(self, req: HistoryRequest):
        """
        Query history bar data from RQData.
        """
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        rq_symbol = self.to_rq_symbol(symbol, exchange)
        if rq_symbol not in self.symbols:
            return None

        rq_interval = INTERVAL_VT2RQ.get(interval)
        if not rq_interval:
            return None

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        adjustment = INTERVAL_ADJUSTMENT_MAP[interval]

        # For querying night trading period data
        end += timedelta(1)

        df = rqdata_get_price(
            rq_symbol,
            frequency=rq_interval,
            fields=["open", "high", "low", "close", "volume"],
            start_date=start,
            end_date=end
        )

        data: List[BarData] = []
        for ix, row in df.iterrows():
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                datetime=row.name.to_pydatetime() - adjustment,
                open_price=row["open"],
                high_price=row["high"],
                low_price=row["low"],
                close_price=row["close"],
                volume=row["volume"],
                gateway_name="RQ"
            )
            data.append(bar)

        return data

    def query_history_tick(self, req: HistoryRequest):
        """
        Query history tick data from RQData.
        """
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        rq_symbol = self.to_rq_symbol(symbol, exchange)
        if rq_symbol not in self.symbols:
            return None

        rq_interval = INTERVAL_VT2RQ.get(interval)
        if not rq_interval:
            return None
        # hard code: download tick data
        rq_interval = "tick"

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        adjustment = INTERVAL_ADJUSTMENT_MAP[interval]

        # For querying night trading period data
        end += timedelta(1)

        df = rqdata_get_price(
            rq_symbol,
            frequency=rq_interval,
            # fields=["open", "high", "low", "close", "volume"],
            start_date=start,
            end_date=end
        )
        print(df)
        print(rq_symbol)
        data: List[TickData] = []
        for ix, row in df.iterrows():
            bar = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=row.name.to_pydatetime() - adjustment,
                name=symbol,
                volume=row["volume"],
                last_price=row["last"],
                last_volume=0.0,
                limit_up=row["limit_up"],
                limit_down=row["limit_down"],
                open_price=row["open"],
                high_price=row["high"],
                low_price=row["low"],
                pre_close=row["prev_close"],
                bid_price_1=row["b1"],
                bid_price_2=row["b2"],
                bid_price_3=row["b3"],
                bid_price_4=row["b4"],
                bid_price_5=row["b5"],
                ask_price_1=row["a1"],
                ask_price_2=row["a2"],
                ask_price_3=row["a3"],
                ask_price_4=row["a4"],
                ask_price_5=row["a5"],
                bid_volume_1=row["b1_v"],
                bid_volume_2=row["b2_v"],
                bid_volume_3=row["b3_v"],
                bid_volume_4=row["b4_v"],
                bid_volume_5=row["b5_v"],
                ask_volume_1=row["a1_v"],
                ask_volume_2=row["a2_v"],
                ask_volume_3=row["a3_v"],
                ask_volume_4=row["a4_v"],
                ask_volume_5=row["a5_v"],
                gateway_name="RQ"
            )
            data.append(bar)

        return data

    def query_history_tick_df(self, req: HistoryRequest):
        """
        Query history tick data from RQData. return Pandas.DataFrame
        """
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        rq_symbol = self.to_rq_symbol(symbol, exchange)
        if rq_symbol not in self.symbols:
            return None

        rq_interval = INTERVAL_VT2RQ.get(interval)
        if not rq_interval:
            return None
        # hard code: download tick data
        rq_interval = "tick"

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        adjustment = INTERVAL_ADJUSTMENT_MAP[interval]

        # For querying night trading period data
        end += timedelta(1)

        df = rqdata_get_price(
            rq_symbol,
            frequency=rq_interval,
            # fields=["open", "high", "low", "close", "volume"],
            start_date=start,
            end_date=end
        )
        return df


rqdata_client = RqdataClient()

# symbol: str
# exchange: Exchange
# datetime: datetime
#
# name: str = ""
# volume: float = 0
# last_price: float = 0
# last_volume: float = 0
# limit_up: float = 0
# limit_down: float = 0
#
# open_price: float = 0
# high_price: float = 0
# low_price: float = 0
# pre_close: float = 0
#
# bid_price_1: float = 0
# bid_price_2: float = 0
# bid_price_3: float = 0
# bid_price_4: float = 0
# bid_price_5: float = 0
#
# ask_price_1: float = 0
# ask_price_2: float = 0
# ask_price_3: float = 0
# ask_price_4: float = 0
# ask_price_5: float = 0
#
# bid_volume_1: float = 0
# bid_volume_2: float = 0
# bid_volume_3: float = 0
# bid_volume_4: float = 0
# bid_volume_5: float = 0
#
# ask_volume_1: float = 0
# ask_volume_2: float = 0
# ask_volume_3: float = 0
# ask_volume_4: float = 0
# ask_volume_5: float = 0

# EQUITIES_TICK_FIELDS = [
#         "trading_date", "open", "last", "high", "low",
#         "prev_close", "volume", "total_turnover", "limit_up", "limit_down",
#         "a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3", "b4", "b5", "a1_v", "a2_v", "a3_v",
#         "a4_v", "a5_v", "b1_v", "b2_v", "b3_v", "b4_v", "b5_v", "change_rate",
#     ]