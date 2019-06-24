""""""
from datetime import datetime
from typing import List, Optional, Sequence, Type
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import get_file_path
from .database import BaseDatabaseManager, Driver
import pandas as pd


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
        col_index = dict(zip(df.columns.values, range(len(df.columns.values))))
        date_time = df.index
        df_array = df.values
        volume_index, last_index, limit_up_index, limit_down_index = col_index["volume"], col_index["last"], col_index["limit_up"], col_index["limit_down"]
        open_index, high_index, low_index, prev_close_index = col_index["open"], col_index["high"], col_index["low"], col_index["prev_close"]
        b1_index, b2_index, b3_index, b4_index, b5_index = col_index["b1"], col_index["b2"], col_index["b3"], col_index["b4"], col_index["b5"]
        a1_index, a2_index, a3_index, a4_index, a5_index = col_index["a1"], col_index["a2"], col_index["a3"], col_index["a4"], col_index["a5"]
        b1_v_index, b2_v_index, b3_v_index, b4_v_index, b5_v_index = col_index["b1_v"], col_index["b2_v"], col_index["b3_v"], col_index["b4_v"], col_index["b5_v"]
        a1_v_index, a2_v_index, a3_v_index, a4_v_index, a5_v_index = col_index["a1_v"], col_index["a2_v"], col_index["a3_v"], col_index["a4_v"], col_index["a5_v"]
        data = []
        for i in range(df_array.shape[0]):
            tick = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=date_time[i],
                name=symbol,
                volume=df_array[i][volume_index],
                last_price=df_array[i][last_index],
                last_volume=0.0,
                limit_up=df_array[i][limit_up_index],
                limit_down=df_array[i][limit_down_index],
                open_price=df_array[i][open_index],
                high_price=df_array[i][high_index],
                low_price=df_array[i][low_index],
                pre_close=df_array[i][prev_close_index],
                bid_price_1=df_array[i][b1_index],
                bid_price_2=df_array[i][b2_index],
                bid_price_3=df_array[i][b3_index],
                bid_price_4=df_array[i][b4_index],
                bid_price_5=df_array[i][b5_index],
                ask_price_1=df_array[i][a1_index],
                ask_price_2=df_array[i][a2_index],
                ask_price_3=df_array[i][a3_index],
                ask_price_4=df_array[i][a4_index],
                ask_price_5=df_array[i][a5_index],
                bid_volume_1=df_array[i][b1_v_index],
                bid_volume_2=df_array[i][b2_v_index],
                bid_volume_3=df_array[i][b3_v_index],
                bid_volume_4=df_array[i][b4_v_index],
                bid_volume_5=df_array[i][b5_v_index],
                ask_volume_1=df_array[i][a1_v_index],
                ask_volume_2=df_array[i][a2_v_index],
                ask_volume_3=df_array[i][a3_v_index],
                ask_volume_4=df_array[i][a4_v_index],
                ask_volume_5=df_array[i][a5_v_index],
                gateway_name="DB"
            )
            data.append(tick)
        h5_store.close()
        return data

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