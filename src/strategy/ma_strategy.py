#!/usr/bin/env python
# encoding: utf-8
'''
来自  https://github.com/Delta-F/DeltaTrader
@desc:双均线策略
'''
import datetime

import strategy.strategy_assess as strat
import data.data_utils as data_utils
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta


def ma_strategy(data, short_window=5, long_window=20):
    """
    双均线策略
    :param data: dataframe, 投资标的行情数据（必须包含收盘价）
    :param short_window: 短期n日移动平均线，默认5
    :param long_window: 长期n日移动平均线，默认20
    :return: 
    """
    print("==========当前周期参数对：", short_window, long_window)

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
    print(data.iloc[-1]["cum_profit"])
    return data


if __name__ == '__main__':

    # 股票列表
    # stocks = ['600887.xshg']#伊利
    stocks = ['000651.xshe']  # 格力
    # stocks = ['600036.xshg']  # 招商600036
    # stocks = ['601318.xshg']  # 平安保险
    # stocks = ['000002.xshg']  # 万科

    # 存放累计收益率
    cum_profits = pd.DataFrame()
    time_tuple = ([5, 10], [5, 20], [5, 30], [5, 60], [5, 120], [5, 250],
                  [10, 20], [10, 30], [10, 60], [10, 120], [10, 250],
                  [20, 30], [20, 60], [20, 120], [20, 250],
                  [30, 60], [30, 120], [30, 250],
                  [60, 120], [60, 250],
                  [120, 250])
    time_line_style = {5: "-", 10: "--", 20: "-.", 30: ":", 60: "dashed", 120: "dotted"}
    years_times = [3, 5, 7, 10, 15]
    end_date: datetime = datetime.datetime.now().date()
    # 循环获取数据
    for years_time in years_times:
        for code in stocks:
            start_date = (end_date + relativedelta(years=-years_time))
            for item in time_tuple:
                df = data_utils.get_single_price(code, 'daily', start_date, end_date)
                df = ma_strategy(df, item[0], item[1])  # 调用双均线策略
                cum_profits[code + str(item)] = df['cum_profit'].reset_index(drop=True)  # 存储累计收益率
                cum_profits[code + str(item)].plot(ls="--")
                # 折线图
                df['cum_profit'].plot(label=code + str(item), ls=time_line_style[item[0]])
                # 筛选有信号点
                df = df[df['signal'] != 0]
                # 预览数据
                print("开仓次数：", int(len(df)))
                # print(df[['close', 'signal', 'pro   、fit_pct', 'cum_profit']])
            cum_profits.plot(figsize=(12, 12),ls="--")
            plt.legend()
            plt.title('Comparison of Ma Strategy Profits' + code + "-" + str(years_time) + "year")
            plt.savefig(r"../../img/" + stocks[0] + "bar_img" + str(start_date) + "-" + str(end_date) + ".png",
                        transparent=True)
            plt.show()

        # 预览
        # print(cum_profits)
        # 可视化
        # cum_profits.plot()
        # plt.legend()
        # plt.title('Comparison of Ma Strategy Profits')
        # plt.savefig(r"../../img/" + stocks[0] + "bar_img" + start_date + "-" + end_date + ".png", transparent=True)
        # plt.show()
        print("双均线策略结束code_str:" + stocks[0])
