#!/usr/bin/env python
# encoding: utf-8
'''
来自  https://github.com/Delta-F/DeltaTrader
@desc:双均线策略
'''
import datetime
import multiprocessing

import dbutils.pooled_db
from sqlalchemy import create_engine

import strategy.strategy_assess as strat
import data.data_utils as data_utils
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
import data.jq_data_data as jd_data
import threading


def ma_strategy(data, short_window=5, long_window=20):
    """
    双均线策略
    :param data: dataframe, 投资标的行情数据（必须包含收盘价）
    :param short_window: 短期n日移动平均线，默认5
    :param long_window: 长期n日移动平均线，默认20
    :return:
    """
    try:
        # print("==========当前周期参数对：", short_window, long_window)

        data: pd.DataFrame = pd.DataFrame(data)
        # 计算技术指标：ma短期、ma长期
        data['short_ma'] = data['close'].rolling(window=short_window).mean()
        data['long_ma'] = data['close'].rolling(window=long_window).mean()

        # 生成信号：金叉买入、死叉卖出
        data['buy_signal'] = np.where(data['short_ma'] > data['long_ma'], 1, 0)
        data['sell_signal'] = np.where(data['short_ma'] < data['long_ma'], -1, 0)
        # print(data[['close', 'short_ma', 'long_ma', 'buy_signal', 'sell_signal']])

        # 过滤信号：st.compose_signal
        data = strat.compose_signal(data)
        # print(data[['close', 'short_ma', 'long_ma', 'signal']])

        # 计算单次收益
        data = strat.calculate_prof_pct(data)
        # print(data.describe())

        # 计算累计收益
        data = strat.calculate_cum_prof(data)

        # 删除多余的columns
        data.drop(labels=['buy_signal', 'sell_signal'], axis=1)

        # 数据预览
        # print(data[['close', 'short_ma', 'long_ma', 'signal', 'cum_profit']])
        # print(data.iloc[-1]["cum_profit"])
    except Exception:
        pass
    return data


exitFlag = 0


class ma_strategy_thread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self, threadID, name, counter: []):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
        print("Starting " + self.name)
        hu_shen_300(self.counter)
        print("Exiting " + self.name)


def hu_shen_300(stock_range: ()):
    # 股票列表
    # stocks = ['600887.xshg']#伊利
    # stocks = ['000651.xshe']  # 格力
    # stocks = ['600036.xshg']  # 招商600036
    # stocks = ['601318.xshg']  # 平安保险
    # stocks = ['000002.xshg']  # 万科
    stocks = ['000001.XSHE', '000002.XSHE', '000063.XSHE', '000066.XSHE', '000069.XSHE', '000100.XSHE', '000157.XSHE',
              '000166.XSHE', '000301.XSHE', '000333.XSHE', '000338.XSHE', '000408.XSHE', '000425.XSHE', '000538.XSHE',
              '000568.XSHE', '000596.XSHE', '000625.XSHE', '000651.XSHE', '000661.XSHE', '000703.XSHE', '000708.XSHE',
              '000725.XSHE', '000768.XSHE', '000776.XSHE', '000786.XSHE', '000792.XSHE', '000800.XSHE', '000858.XSHE',
              '000876.XSHE', '000877.XSHE', '000895.XSHE', '000938.XSHE', '000963.XSHE', '000977.XSHE', '001289.XSHE',
              '001979.XSHE', '002001.XSHE', '002007.XSHE', '002008.XSHE', '002027.XSHE', '002032.XSHE', '002049.XSHE',
              '002050.XSHE', '002064.XSHE', '002074.XSHE', '002120.XSHE', '002129.XSHE', '002142.XSHE', '002179.XSHE',
              '002202.XSHE', '002230.XSHE', '002236.XSHE', '002241.XSHE', '002252.XSHE', '002271.XSHE', '002304.XSHE',
              '002311.XSHE', '002352.XSHE', '002371.XSHE', '002410.XSHE', '002414.XSHE', '002415.XSHE', '002459.XSHE',
              '002460.XSHE', '002466.XSHE', '002475.XSHE', '002493.XSHE', '002555.XSHE', '002568.XSHE', '002594.XSHE',
              '002600.XSHE', '002601.XSHE', '002602.XSHE', '002607.XSHE', '002648.XSHE', '002709.XSHE', '002714.XSHE',
              '002736.XSHE', '002791.XSHE', '002812.XSHE', '002821.XSHE', '002841.XSHE', '002916.XSHE', '002920.XSHE',
              '002938.XSHE', '003816.XSHE', '300003.XSHE', '300014.XSHE', '300015.XSHE', '300033.XSHE', '300059.XSHE',
              '300122.XSHE', '300124.XSHE', '300142.XSHE', '300207.XSHE', '300223.XSHE', '300274.XSHE', '300316.XSHE',
              '300347.XSHE', '300408.XSHE', '300413.XSHE', '300433.XSHE', '300450.XSHE', '300454.XSHE', '300496.XSHE',
              '300498.XSHE', '300529.XSHE', '300595.XSHE', '300601.XSHE', '300628.XSHE', '300661.XSHE', '300750.XSHE',
              '300751.XSHE', '300759.XSHE', '300760.XSHE', '300763.XSHE', '300782.XSHE', '300866.XSHE', '300896.XSHE',
              '300919.XSHE', '300957.XSHE', '300979.XSHE', '300999.XSHE', '600000.XSHG', '600009.XSHG', '600010.XSHG',
              '600011.XSHG', '600015.XSHG', '600016.XSHG', '600018.XSHG', '600019.XSHG', '600025.XSHG', '600028.XSHG',
              '600029.XSHG', '600030.XSHG', '600031.XSHG', '600036.XSHG', '600048.XSHG', '600050.XSHG', '600061.XSHG',
              '600085.XSHG', '600089.XSHG', '600104.XSHG', '600111.XSHG', '600115.XSHG', '600132.XSHG', '600150.XSHG',
              '600161.XSHG', '600176.XSHG', '600183.XSHG', '600188.XSHG', '600196.XSHG', '600219.XSHG', '600276.XSHG',
              '600309.XSHG', '600332.XSHG', '600346.XSHG', '600352.XSHG', '600362.XSHG', '600383.XSHG', '600406.XSHG',
              '600426.XSHG', '600436.XSHG', '600438.XSHG', '600460.XSHG', '600489.XSHG', '600519.XSHG', '600547.XSHG',
              '600570.XSHG', '600584.XSHG', '600585.XSHG', '600588.XSHG', '600600.XSHG', '600606.XSHG', '600655.XSHG',
              '600660.XSHG', '600690.XSHG', '600741.XSHG', '600745.XSHG', '600760.XSHG', '600763.XSHG', '600795.XSHG',
              '600809.XSHG', '600837.XSHG', '600845.XSHG', '600886.XSHG', '600887.XSHG', '600893.XSHG', '600900.XSHG',
              '600905.XSHG', '600918.XSHG', '600919.XSHG', '600926.XSHG', '600941.XSHG', '600958.XSHG', '600989.XSHG',
              '600999.XSHG', '601006.XSHG', '601009.XSHG', '601012.XSHG', '601021.XSHG', '601066.XSHG', '601088.XSHG',
              '601100.XSHG', '601111.XSHG', '601117.XSHG', '601138.XSHG', '601155.XSHG', '601166.XSHG', '601169.XSHG',
              '601186.XSHG', '601211.XSHG', '601216.XSHG', '601225.XSHG', '601229.XSHG', '601236.XSHG', '601238.XSHG',
              '601288.XSHG', '601318.XSHG', '601319.XSHG', '601328.XSHG', '601336.XSHG', '601360.XSHG', '601377.XSHG',
              '601390.XSHG', '601398.XSHG', '601600.XSHG', '601601.XSHG', '601618.XSHG', '601628.XSHG', '601633.XSHG',
              '601658.XSHG', '601668.XSHG', '601669.XSHG', '601688.XSHG', '601698.XSHG', '601728.XSHG', '601766.XSHG',
              '601788.XSHG', '601799.XSHG', '601800.XSHG', '601808.XSHG', '601816.XSHG', '601818.XSHG', '601825.XSHG',
              '601838.XSHG', '601857.XSHG', '601865.XSHG', '601868.XSHG', '601877.XSHG', '601878.XSHG', '601881.XSHG',
              '601888.XSHG', '601898.XSHG', '601899.XSHG', '601901.XSHG', '601916.XSHG', '601919.XSHG', '601939.XSHG',
              '601966.XSHG', '601985.XSHG', '601988.XSHG', '601989.XSHG', '601995.XSHG', '601998.XSHG', '603019.XSHG',
              '603087.XSHG', '603160.XSHG', '603185.XSHG', '603195.XSHG', '603259.XSHG', '603260.XSHG', '603288.XSHG',
              '603290.XSHG', '603369.XSHG', '603392.XSHG', '603486.XSHG', '603501.XSHG', '603659.XSHG', '603799.XSHG',
              '603806.XSHG', '603833.XSHG', '603882.XSHG', '603899.XSHG', '603986.XSHG', '603993.XSHG', '605499.XSHG',
              '688005.XSHG', '688008.XSHG', '688012.XSHG', '688036.XSHG', '688065.XSHG', '688111.XSHG', '688126.XSHG',
              '688169.XSHG', '688363.XSHG', '688396.XSHG', '688561.XSHG', '688599.XSHG', '688981.XSHG']
    stocks = stocks[stock_range[0]: stock_range[1]]
    print(stocks)
    time_tuple = ([5, 10], [5, 20], [5, 30], [5, 60], [5, 120], [5, 250],
                  [10, 20], [10, 30], [10, 60], [10, 120], [10, 250],
                  [20, 30], [20, 60], [20, 120], [20, 250],
                  [30, 60], [30, 120], [30, 250],
                  [60, 120], [60, 250],
                  [120, 250])
    time_line_style = {5: "-", 10: "--", 20: "-.", 30: ":", 60: "dashed", 120: "dotted"}
    years_times = [3, 5, 7, 10, 15]
    end_date: datetime = datetime.datetime(2022, 11, 9).date()
    # 存结果
    i = 1
    for code in stocks:
        i += 1
        print(i)
        result_data_frame = pd.DataFrame(columns=("code_str", "year", "cycle", "num", "Profits"))
        for years_time in years_times:
            start_date = (end_date + relativedelta(years=-years_time))
            result_row = None
            # 存放累计收益率
            cum_profits = pd.DataFrame()
            for item in time_tuple:
                df = data_utils.get_single_price(code, 'daily', start_date, end_date)
                df = ma_strategy(df, item[0], item[1])  # 调用双均线策略
                cum_profits[code + str(item)] = df['cum_profit'].reset_index(drop=True)  # 存储累计收益率
                # 折线图
                # df['cum_profit'].plot(label=code + str(item), ls=time_line_style[item[0]])
                cum_profits[code + str(item)].plot(label=code + str(item), ls=time_line_style[item[0]],
                                                   figsize=(12, 12))
                # 筛选有信号点
                df = df[df['signal'] != 0]
                # 预览数据
                # print("开仓次数：", int(len(df)))
                result_row = {"code_str": code, "year": years_time, "cycle": item, "num": int(len(df)),
                              "Profits": cum_profits[code + str(item)].iloc[int(len(df)) - 1]}
                result_data_frame = result_data_frame.append(result_row, ignore_index=True)
                # print(df[['close', 'signal', 'pro   、fit_pct', 'cum_profit']])
            # cum_profits.plot(figsize=(12, 12))
            plt.legend()
            plt.plot()
            plt.title('Comparison of Ma Strategy Profits' + code + "-" + str(years_time) + "year")
            # plt.show()
            plt.savefig(
                "../../img/" + code + "_" + str(years_time) + "year_" + str(start_date) + "__" + str(end_date) + ".png",
                transparent=True)
            plt.close()
            # 预览
            # print(cum_profits)
            # 可视化
            # cum_profits.plot()

        # print("双均线策略结束code_str:" + stocks[0])
        # print(result_data_frame)
        # result_data_frame.plot()
        # plt.savefig(r"../../img/" + stocks[0] + "all_Profits_" + "bar_img.png",
        #             transparent=True)
        # plt.show()
        writer = pd.ExcelWriter("../../img/" + code + ".xlsx")
        result_data_frame.to_excel(writer)
        writer.save()
        writer.close()


if __name__ == '__main__':
    # 创建新线程
    # core = multiprocessing.cpu_count() * 2
    # num = 300
    # index = 0
    # last = 0
    # while (core > 0):
    #     if core == 1:
    #         thread = ma_strategy_thread(core, "Thread-" + str(core), (index, 300))
    #     else:
    #         range_num = int(300 / core)
    #         last = index + range_num
    #         thread = ma_strategy_thread(core, "Thread-" + str(core), (index, last))
    #         index += range_num
    #         core -= 1
    #         # 开启线程
    #         thread.start()
    # hu_shen_300((0, 301))
    # 数据统计

    pd.DataFrame()
