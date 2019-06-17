""""""
from datetime import datetime
from typing import List, Optional, Sequence, Type
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import get_file_path
from .database import BaseDatabaseManager, Driver
import pandas as pd
import time


class Hdf5Manager(BaseDatabaseManager):
    def __init__(self, database_name: str):
        self.database_name = database_name

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime,
    ) -> Sequence[BarData]:
        pass

    def load_tick_data(
            self,
            symbol: str,
            exchange: Exchange,
            start: datetime,
            end: datetime,
    ) -> Sequence[TickData]:
        h5_store = pd.HDFStore(self.database_name, "r")
        # 选择出符合条件的数据
        df = h5_store.select("tick",where=['trading_date>=Timestamp("'+ start.strftime("%Y-%m-%d %H:%M:%S") +\
                            '") and trading_date<=Timestamp("' + end.strftime("%Y-%m-%d %H:%M:%S") + '")'])
        h5_store.close()
        return df
        # data = []
        # for ix, row in df.iterrows():
        #     tick = TickData(
        #         symbol=symbol,
        #         exchange=exchange,
        #         datetime=ix,
        #         name=symbol,
        #         volume=row["volume"],
        #         last_price=row["last"],
        #         last_volume=0.0,
        #         limit_up=row["limit_up"],
        #         limit_down=row["limit_down"],
        #         open_price=row["open"],
        #         high_price=row["high"],
        #         low_price=row["low"],
        #         pre_close=row["prev_close"],
        #         bid_price_1=row["b1"],
        #         bid_price_2=row["b2"],
        #         bid_price_3=row["b3"],
        #         bid_price_4=row["b4"],
        #         bid_price_5=row["b5"],
        #         ask_price_1=row["a1"],
        #         ask_price_2=row["a2"],
        #         ask_price_3=row["a3"],
        #         ask_price_4=row["a4"],
        #         ask_price_5=row["a5"],
        #         bid_volume_1=row["b1_v"],
        #         bid_volume_2=row["b2_v"],
        #         bid_volume_3=row["b3_v"],
        #         bid_volume_4=row["b4_v"],
        #         bid_volume_5=row["b5_v"],
        #         ask_volume_1=row["a1_v"],
        #         ask_volume_2=row["a2_v"],
        #         ask_volume_3=row["a3_v"],
        #         ask_volume_4=row["a4_v"],
        #         ask_volume_5=row["a5_v"],
        #         gateway_name="DB"
        #     )
        #     data.append(tick)
        # h5_store.close()
        # return data

    def save_bar_data(self, datas):
        """
        save bar data to hdf5 file
        :param datas: Pandas.DataFrame
        :return: boolean
        """
        try:
            h5_store = pd.HDFStore(self.database_name, "a")
            h5_store.append("bar", datas, format="table", append=False, data_columns=True)
            return True
        except Exception as e:
            print()
            return False

    def save_tick_data(self, datas):
        try:
            h5_store = pd.HDFStore(self.database_name, "a")
            h5_store.append("tick", datas, format="table", append=False, data_columns=True)
            h5_store.close()
            return True
        except Exception as e:
            print("{}".format(str(e)))
            return False

    def get_newest_bar_data(
        self,
        symbol: str,
        exchange: "Exchange",
        interval: "Interval"
    ):
        pass

    def get_newest_tick_data(
        self,
        symbol: str,
        exchange: "Exchange",
    ):
        pass

    def clean(self, symbol: str):
        pass