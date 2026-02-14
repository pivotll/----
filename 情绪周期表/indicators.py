"""
指标计算模块
负责计算所有市场情绪指标
"""

import pandas as pd
import numpy as np
from typing import Dict


class IndicatorCalculator:
    """市场情绪指标计算器"""
    
    def __init__(self):
        """初始化"""
        pass
    
    def calculate_indicators(self, data: dict, prev_data: dict = None) -> dict:
        """
        计算指定日期的所有指标
        
        Args:
            data: 当日数据字典（包含daily, limit_data等）
            prev_data: 前一日数据字典（用于计算晋级率等）
            
        Returns:
            指标字典
        """
        trade_date = data['trade_date']
        daily_df = data['daily']
        limit_data_df = data['limit_data']
        # 昨日日线数据（用于反包等跨日指标）
        prev_daily_df = pd.DataFrame()
        if prev_data is not None and isinstance(prev_data, dict):
            prev_daily_df = prev_data.get('daily', pd.DataFrame())
        
        # 根据limit字段分离涨停、跌停、炸板数据
        if not limit_data_df.empty and 'limit' in limit_data_df.columns:
            limit_up_df = limit_data_df[limit_data_df['limit'] == 'U'].copy()
            limit_down_df = limit_data_df[limit_data_df['limit'] == 'D'].copy()
            break_board_df = limit_data_df[limit_data_df['limit'] == 'Z'].copy()
        else:
            limit_up_df = pd.DataFrame()
            limit_down_df = pd.DataFrame()
            break_board_df = pd.DataFrame()
        
        # 合并涨停数据与日线数据（获取open, high, low等字段）
        if not limit_up_df.empty and not daily_df.empty:
            limit_up_df = limit_up_df.merge(
                daily_df[['ts_code', 'open', 'high', 'low', 'pre_close']],
                on='ts_code',
                how='left'
            )
        
        indicators = {
            'trade_date': trade_date
        }
        
        # 1. 市场基础指标
        indicators.update(self._calc_market_basic(daily_df))
        
        # 2. 涨停跌停统计
        indicators.update(self._calc_limit_stats(limit_up_df, limit_down_df, break_board_df))
        
        # 3. 连板统计
        indicators.update(self._calc_board_stats(limit_up_df))
        
        # 4. 高阶指标（包含反包、涨停金额、封单金额等）
        indicators.update(self._calc_advanced_indicators(daily_df, limit_up_df, prev_daily_df))
        
        # 5. 红盘率和溢价（当日涨停板的表现）
        indicators.update(self._calc_red_rate_premium(limit_up_df, daily_df))
        
        # 6. 昨日涨停今日表现（需要前一日数据）
        if prev_data is not None and not prev_data.get('limit_data', pd.DataFrame()).empty:
            # 从前一日数据中提取涨停数据
            prev_limit_data = prev_data['limit_data']
            if not prev_limit_data.empty and 'limit' in prev_limit_data.columns:
                prev_limit_up_df = prev_limit_data[prev_limit_data['limit'] == 'U'].copy()
                indicators.update(self._calc_yesterday_performance(prev_limit_up_df, daily_df))
        
        # 7. 晋级率（需要前一日数据）
        if prev_data is not None and not prev_data.get('limit_data', pd.DataFrame()).empty:
            # 从前一日数据中提取涨停数据
            prev_limit_data = prev_data['limit_data']
            if not prev_limit_data.empty and 'limit' in prev_limit_data.columns:
                prev_limit_up_df = prev_limit_data[prev_limit_data['limit'] == 'U'].copy()
                indicators.update(self._calc_advance_rate(limit_up_df, prev_limit_up_df, daily_df, prev_data.get('daily', pd.DataFrame())))
            else:
                indicators.update({
                    'advance_1to2': None,
                    'advance_2to3': None,
                    'advance_3to4': None,
                    'advance_3plus': None
                })
        else:
            indicators.update({
                'advance_1to2': None,
                'advance_2to3': None,
                'advance_3to4': None,
                'advance_3plus': None
            })
        
        return indicators
    
    def _calc_market_basic(self, daily_df: pd.DataFrame) -> dict:
        """计算市场基础指标"""
        if daily_df.empty:
            return {
                'up_count': 0,
                'down_count': 0,
                'up5_count': 0,
                'down5_count': 0
            }
        
        # 上涨/下跌家数
        up_count = len(daily_df[daily_df['pct_chg'] > 0])
        down_count = len(daily_df[daily_df['pct_chg'] < 0])
        
        # 涨跌5%以上
        up5_count = len(daily_df[daily_df['pct_chg'] >= 5])
        down5_count = len(daily_df[daily_df['pct_chg'] <= -5])
        
        return {
            'up_count': up_count,
            'down_count': down_count,
            'up5_count': up5_count,
            'down5_count': down5_count
        }
    
    def _calc_limit_stats(self, limit_up_df: pd.DataFrame, 
                          limit_down_df: pd.DataFrame,
                          break_board_df: pd.DataFrame) -> dict:
        """计算涨停跌停统计"""
        limit_up_count = len(limit_up_df)
        limit_down_count = len(limit_down_df)
        break_count = len(break_board_df)
        
        # 炸板率 = 炸板数 / (涨停数 + 炸板数)
        if limit_up_count + break_count > 0:
            break_rate = break_count / (limit_up_count + break_count) * 100
        else:
            break_rate = 0
        
        return {
            'limit_up_count': limit_up_count,
            'limit_down_count': limit_down_count,
            'break_count': break_count,
            'break_rate': round(break_rate, 2)
        }
    
    def _calc_board_stats(self, limit_up_df: pd.DataFrame) -> dict:
        """计算连板统计"""
        if limit_up_df.empty:
            return {
                'first_board': 0,
                'second_board': 0,
                'third_board': 0,
                'above_third': 0,
                'max_board': 0
            }
        
        # 连板数统计
        first_board = len(limit_up_df[limit_up_df['limit_times'] == 1])
        second_board = len(limit_up_df[limit_up_df['limit_times'] == 2])
        third_board = len(limit_up_df[limit_up_df['limit_times'] == 3])
        above_third = len(limit_up_df[limit_up_df['limit_times'] >= 3])
        
        # 最高板
        max_board = int(limit_up_df['limit_times'].max()) if not limit_up_df['limit_times'].isna().all() else 0
        
        return {
            'first_board': first_board,
            'second_board': second_board,
            'third_board': third_board,
            'above_third': above_third,
            'max_board': max_board
        }
    
    def _calc_advanced_indicators(self, daily_df: pd.DataFrame,
                                   limit_up_df: pd.DataFrame,
                                   prev_daily_df: pd.DataFrame) -> dict:
        """
        计算高阶指标

        - 反包涨停：今日涨停，且今天最低价 < 昨日最低价
          （需要用到昨日日线数据 prev_daily_df）
        """
        if limit_up_df.empty:
            return {
                'fanpao_count': 0,
                'limit_amount': 0,
                'seal_amount': 0
            }
        
        # 反包涨停：今日涨停且最低价 < 昨日最低价
        fanpao_count = 0
        if not prev_daily_df.empty and 'low' in limit_up_df.columns:
            # 取昨日日线中的最低价
            prev_low = prev_daily_df[['ts_code', 'low']].dropna().rename(columns={'low': 'yesterday_low'})
            merged = limit_up_df.merge(prev_low, on='ts_code', how='left')
            if 'yesterday_low' in merged.columns:
                fanpao_mask = (merged['yesterday_low'].notna()) & (merged['low'] < merged['yesterday_low'])
                fanpao_count = int(fanpao_mask.sum())
        
        # 涨停金额（单位：亿元）
        if 'amount' in limit_up_df.columns:
            # 这里按tushare实际返回数据校验后处理：
            # 原始amount为“元”级别时，转换为“亿元”应除以1e8
            limit_amount = limit_up_df['amount'].sum() / 1e8
        else:
            limit_amount = 0
        
        # 封单金额（单位：亿元）
        if 'fd_amount' in limit_up_df.columns:
            # 同上，按“元”级别转换为“亿元”
            seal_amount = limit_up_df['fd_amount'].sum() / 1e8
        else:
            seal_amount = 0
        
        return {
            'fanpao_count': fanpao_count,
            'limit_amount': round(limit_amount, 2),
            'seal_amount': round(seal_amount, 2)
        }
    
    def _calc_red_rate_premium(self, limit_up_df: pd.DataFrame, daily_df: pd.DataFrame) -> dict:
        """
        计算红盘率和溢价
        
        红盘率：开盘价 >= 昨收价的比例
        溢价：收盘价相对昨收价的涨幅
        """
        result = {
            'first_red_rate': None,
            'first_premium': None,
            'second_red_rate': None,
            'second_premium': None,
            'third_red_rate': None,
            'third_premium': None
        }
        
        if limit_up_df.empty:
            return result
        
        # 首板
        first_df = limit_up_df[limit_up_df['limit_times'] == 1]
        if len(first_df) > 0:
            # 红盘率：开盘价 >= 昨收价的比例
            if 'open' in first_df.columns and 'pre_close' in first_df.columns:
                red_count = len(first_df[first_df['open'] >= first_df['pre_close']])
                result['first_red_rate'] = round(red_count / len(first_df) * 100, 2)
            
            # 溢价：平均涨幅
            if 'pct_chg' in first_df.columns:
                result['first_premium'] = round(first_df['pct_chg'].mean(), 2)
        
        # 二板
        second_df = limit_up_df[limit_up_df['limit_times'] == 2]
        if len(second_df) > 0:
            if 'open' in second_df.columns and 'pre_close' in second_df.columns:
                red_count = len(second_df[second_df['open'] >= second_df['pre_close']])
                result['second_red_rate'] = round(red_count / len(second_df) * 100, 2)
            
            if 'pct_chg' in second_df.columns:
                result['second_premium'] = round(second_df['pct_chg'].mean(), 2)
        
        # 三板以上
        third_df = limit_up_df[limit_up_df['limit_times'] >= 3]
        if len(third_df) > 0:
            if 'open' in third_df.columns and 'pre_close' in third_df.columns:
                red_count = len(third_df[third_df['open'] >= third_df['pre_close']])
                result['third_red_rate'] = round(red_count / len(third_df) * 100, 2)
            
            if 'pct_chg' in third_df.columns:
                result['third_premium'] = round(third_df['pct_chg'].mean(), 2)
        
        return result
    
    def _calc_yesterday_performance(self, yesterday_limit_up_df: pd.DataFrame, 
                                     today_daily_df: pd.DataFrame) -> dict:
        """
        计算昨日涨停股票今日的表现（用于红盘率和溢价）
        
        口径（全部基于“昨日涨停，今日涨幅”）：
        - 首板红盘%   = 昨日首板股票中，今日涨幅 > 0 的比例
        - 首板溢价%   = 昨日首板股票今日平均涨幅
        - 二板红盘%   = 昨日二板股票中，今日涨幅 > 0 的比例
        - 二板溢价%   = 昨日二板股票今日平均涨幅
        - 三板红盘%   = 昨日三板股票中，今日涨幅 > 0 的比例
        - 三板溢价%   = 昨日三板股票今日平均涨幅
        - 3板+红盘%  = 昨日3板及以上股票中，今日涨幅 > 0 的比例
        - 3板+溢价%  = 昨日3板及以上股票今日平均涨幅
        """
        result = {
            'first_red_rate': None,
            'first_premium': None,
            'second_red_rate': None,
            'second_premium': None,
            'third_red_rate': None,
            'third_premium': None,
            'third_plus_red_rate': None,
            'third_plus_premium': None,
        }
        
        if yesterday_limit_up_df.empty or today_daily_df.empty:
            return result
        
        # 昨日首板股票
        yesterday_first = yesterday_limit_up_df[yesterday_limit_up_df['limit_times'] == 1]['ts_code'].tolist()
        if yesterday_first:
            today_performance = today_daily_df[today_daily_df['ts_code'].isin(yesterday_first)]
            if not today_performance.empty:
                # 溢价：今日平均涨幅
                result['first_premium'] = round(today_performance['pct_chg'].mean(), 2)
                # 红盘率：今日涨幅 > 0 的比例
                red_count = len(today_performance[today_performance['pct_chg'] > 0])
                result['first_red_rate'] = round(red_count / len(today_performance) * 100, 2)
        
        # 昨日二板股票
        yesterday_second = yesterday_limit_up_df[yesterday_limit_up_df['limit_times'] == 2]['ts_code'].tolist()
        if yesterday_second:
            today_performance = today_daily_df[today_daily_df['ts_code'].isin(yesterday_second)]
            if not today_performance.empty:
                result['second_premium'] = round(today_performance['pct_chg'].mean(), 2)
                red_count = len(today_performance[today_performance['pct_chg'] > 0])
                result['second_red_rate'] = round(red_count / len(today_performance) * 100, 2)
        
        # 昨日三板（严格 =3）
        yesterday_third = yesterday_limit_up_df[yesterday_limit_up_df['limit_times'] == 3]['ts_code'].tolist()
        if yesterday_third:
            today_performance = today_daily_df[today_daily_df['ts_code'].isin(yesterday_third)]
            if not today_performance.empty:
                result['third_premium'] = round(today_performance['pct_chg'].mean(), 2)
                red_count = len(today_performance[today_performance['pct_chg'] > 0])
                result['third_red_rate'] = round(red_count / len(today_performance) * 100, 2)

        # 昨日3板及以上（3板+）
        yesterday_third_plus = yesterday_limit_up_df[yesterday_limit_up_df['limit_times'] >= 3]['ts_code'].tolist()
        if yesterday_third_plus:
            today_performance = today_daily_df[today_daily_df['ts_code'].isin(yesterday_third_plus)]
            if not today_performance.empty:
                result['third_plus_premium'] = round(today_performance['pct_chg'].mean(), 2)
                red_count = len(today_performance[today_performance['pct_chg'] > 0])
                result['third_plus_red_rate'] = round(red_count / len(today_performance) * 100, 2)
        
        return result
    
    def _calc_advance_rate(self, today_limit_df: pd.DataFrame, 
                           yesterday_limit_df: pd.DataFrame,
                           today_daily_df: pd.DataFrame,
                           yesterday_daily_df: pd.DataFrame) -> dict:
        """
        计算晋级率（修正版）
        
        算法说明（分母仅统计「昨日N板且今日有交易」的股票，避免停牌拉低晋级率）：
        - 1进2%: (昨日首板且今日再次涨停数) / (昨日首板且今日有交易数) × 100
        - 2进3%: (昨日二板且今日再次涨停数) / (昨日二板且今日有交易数) × 100
        - 3进4%: (昨日三板且今日再次涨停数) / (昨日三板且今日有交易数) × 100
        - 3板+晋级%: (昨日四板及以上且今日再次涨停数) / (昨日四板及以上且今日有交易数) × 100
        
        limit_times 含义（tushare）：当日连续涨停天数，1=首板，2=二板，3=三板，4+=四板及以上。
        """
        result = {
            'advance_1to2': None,   # 昨日首板 → 今日任意涨停
            'advance_2to3': None,   # 昨日二板 → 今日任意涨停
            'advance_3to4': None,   # 昨日三板 → 今日任意涨停（3进4）
            'advance_3plus': None   # 昨日四板及以上 → 今日任意涨停（3板+晋级，不含3板）
        }
        
        if today_limit_df.empty or yesterday_limit_df.empty:
            return result
        
        # 今日有交易的股票（用于分母：只统计“有机会晋级”的昨日涨停股，排除今日停牌）
        today_traded = set(today_daily_df['ts_code']) if not today_daily_df.empty else set()
        # 今日所有涨停股票
        today_all_limit = set(today_limit_df['ts_code'])
        
        # 昨日首板股票；分母 = 昨日首板且今日有交易
        yesterday_1b = set(yesterday_limit_df[yesterday_limit_df['limit_times'] == 1]['ts_code'])
        yesterday_1b_traded = yesterday_1b & today_traded if today_traded else yesterday_1b
        if len(yesterday_1b_traded) > 0:
            advance_1to2_count = len(yesterday_1b_traded & today_all_limit)
            result['advance_1to2'] = round(advance_1to2_count / len(yesterday_1b_traded) * 100, 2)
        
        # 昨日二板
        yesterday_2b = set(yesterday_limit_df[yesterday_limit_df['limit_times'] == 2]['ts_code'])
        yesterday_2b_traded = yesterday_2b & today_traded if today_traded else yesterday_2b
        if len(yesterday_2b_traded) > 0:
            advance_2to3_count = len(yesterday_2b_traded & today_all_limit)
            result['advance_2to3'] = round(advance_2to3_count / len(yesterday_2b_traded) * 100, 2)
        
        # 昨日三板（严格 =3）
        yesterday_3b = set(yesterday_limit_df[yesterday_limit_df['limit_times'] == 3]['ts_code'])
        yesterday_3b_traded = yesterday_3b & today_traded if today_traded else yesterday_3b
        if len(yesterday_3b_traded) > 0:
            advance_3to4_count = len(yesterday_3b_traded & today_all_limit)
            result['advance_3to4'] = round(advance_3to4_count / len(yesterday_3b_traded) * 100, 2)
        
        # 昨日四板及以上（>=4）
        yesterday_3plus = set(yesterday_limit_df[yesterday_limit_df['limit_times'] >= 4]['ts_code'])
        yesterday_3plus_traded = yesterday_3plus & today_traded if today_traded else yesterday_3plus
        if len(yesterday_3plus_traded) > 0:
            advance_3plus_count = len(yesterday_3plus_traded & today_all_limit)
            result['advance_3plus'] = round(advance_3plus_count / len(yesterday_3plus_traded) * 100, 2)
        
        return result


def test_indicator_calculator():
    """测试指标计算"""
    print("=" * 60)
    print("测试指标计算模块")
    print("=" * 60)
    
    # 创建模拟数据
    daily_data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ', '000003.SZ', '000004.SZ'],
        'pct_chg': [5.2, -3.1, 8.5, -6.2],
        'amount': [150000, 80000, 200000, 120000]
    })
    
    limit_up_data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ'],
        'limit_times': [1, 2],
        'amount': [150000, 200000],
        'fd_amount': [50000, 80000],
        'open': [10.0, 20.0],
        'close': [11.0, 22.0],
        'low': [9.8, 19.5],
        'pre_close': [10.0, 20.0],
        'pct_chg': [10.0, 10.0]
    })
    
    data = {
        'trade_date': '20260120',
        'daily': daily_data,
        'limit_up': limit_up_data,
        'limit_down': pd.DataFrame(),
        'break_board': pd.DataFrame()
    }
    
    calculator = IndicatorCalculator()
    indicators = calculator.calculate_indicators(data)
    
    print("\n计算结果：")
    for key, value in indicators.items():
        print(f"  {key}: {value}")
    
    print("\n✅ 测试完成")


if __name__ == '__main__':
    test_indicator_calculator()
