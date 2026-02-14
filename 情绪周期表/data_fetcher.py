"""
数据获取模块
负责从tushare获取原始数据
"""

import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import List, Tuple
import config


class DataFetcher:
    """tushare数据获取器"""
    
    def __init__(self):
        """初始化tushare连接"""
        ts.set_token(config.TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        print("✅ Tushare API连接成功")
    
    def get_trade_cal(self, start_date: str, end_date: str) -> List[str]:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            
        Returns:
            交易日列表
        """
        try:
            df = self.pro.trade_cal(
                exchange='SSE',
                start_date=start_date,
                end_date=end_date,
                is_open='1'  # 只获取交易日
            )
            
            if df is None or df.empty:
                print(f"⚠️  {start_date}至{end_date}期间无交易日")
                return []
            
            trade_dates = df['cal_date'].tolist()
            print(f"📅 获取到{len(trade_dates)}个交易日")
            return trade_dates
            
        except Exception as e:
            print(f"❌ 获取交易日历失败: {e}")
            return []
    
    def fetch_daily_data(self, trade_date: str) -> pd.DataFrame:
        """
        获取指定日期的全市场日线行情数据
        
        Args:
            trade_date: 交易日期，格式：YYYYMMDD
            
        Returns:
            日线行情DataFrame
        """
        try:
            time.sleep(config.API_DELAY)
            
            df = self.pro.daily(trade_date=trade_date)
            
            if df is None or df.empty:
                print(f"⚠️  {trade_date} 无日线数据")
                return pd.DataFrame()
            
            print(f"  📊 {trade_date} 获取日线数据: {len(df)}条")
            return df
            
        except Exception as e:
            print(f"❌ 获取日线数据失败 ({trade_date}): {e}")
            return pd.DataFrame()
    
    def fetch_limit_list_all(self, trade_date: str) -> pd.DataFrame:
        """
        获取指定日期的所有涨停/跌停/炸板数据（一次性获取）
        
        Args:
            trade_date: 交易日期，格式：YYYYMMDD
            
        Returns:
            涨跌停数据DataFrame，包含limit字段（U=涨停，D=跌停，Z=炸板）
        """
        try:
            time.sleep(config.API_DELAY)
            
            df = self.pro.limit_list_d(
                trade_date=trade_date,
                fields='ts_code,trade_date,industry,name,close,pct_chg,amount,'
                       'limit_amount,float_mv,total_mv,turnover_ratio,fd_amount,'
                       'first_time,last_time,open_times,up_stat,limit_times,limit'
            )
            
            if df is None or df.empty:
                # 涨停/跌停数据为空是正常的（可能当天没有）
                return pd.DataFrame()
            
            # 统计各类型数量
            limit_u = len(df[df['limit'] == 'U']) if 'limit' in df.columns else 0
            limit_d = len(df[df['limit'] == 'D']) if 'limit' in df.columns else 0
            limit_z = len(df[df['limit'] == 'Z']) if 'limit' in df.columns else 0
            
            print(f"  🔥 {trade_date} 涨跌停数据: 涨停{limit_u}只 跌停{limit_d}只 炸板{limit_z}只")
            return df
            
        except Exception as e:
            print(f"❌ 获取涨跌停数据失败 ({trade_date}): {e}")
            return pd.DataFrame()
    
    def fetch_daily_basic(self, trade_date: str) -> pd.DataFrame:
        """
        获取指定日期的每日基础指标数据
        
        Args:
            trade_date: 交易日期，格式：YYYYMMDD
            
        Returns:
            每日基础指标DataFrame
        """
        try:
            time.sleep(config.API_DELAY)
            
            df = self.pro.daily_basic(
                trade_date=trade_date,
                fields='ts_code,trade_date,turnover_rate,turnover_rate_f,'
                       'volume_ratio,total_mv,circ_mv'
            )
            
            if df is None or df.empty:
                print(f"⚠️  {trade_date} 无基础指标数据")
                return pd.DataFrame()
            
            print(f"  📈 {trade_date} 获取基础指标: {len(df)}条")
            return df
            
        except Exception as e:
            print(f"❌ 获取基础指标失败 ({trade_date}): {e}")
            return pd.DataFrame()
    
    def fetch_all_data_for_date(self, trade_date: str) -> dict:
        """
        获取指定日期的所有数据
        
        Args:
            trade_date: 交易日期，格式：YYYYMMDD
            
        Returns:
            包含所有数据的字典
        """
        print(f"\n🔍 开始获取 {trade_date} 的数据...")
        
        data = {
            'trade_date': trade_date,
            'daily': pd.DataFrame(),
            'limit_data': pd.DataFrame(),  # 所有涨跌停炸板数据
            'daily_basic': pd.DataFrame()
        }
        
        # 获取日线数据
        data['daily'] = self.fetch_daily_data(trade_date)
        
        # 获取所有涨跌停炸板数据（一次性获取）
        data['limit_data'] = self.fetch_limit_list_all(trade_date)
        
        # 获取基础指标
        data['daily_basic'] = self.fetch_daily_basic(trade_date)
        
        print(f"✅ {trade_date} 数据获取完成\n")
        return data
    
    def fetch_batch_data(self, start_date: str, end_date: str) -> List[dict]:
        """
        批量获取指定日期范围的数据
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            
        Returns:
            数据列表
        """
        # 获取交易日历
        trade_dates = self.get_trade_cal(start_date, end_date)

        if not trade_dates:
            print("❌ 无可用交易日")
            return []

        # 为保证晋级率等跨日指标正确，按时间先后顺序排序
        trade_dates = sorted(trade_dates)

        all_data = []
        total = len(trade_dates)
        
        print(f"\n🚀 开始批量获取数据，共{total}个交易日\n")
        print("=" * 60)
        
        for idx, trade_date in enumerate(trade_dates, 1):
            print(f"\n[{idx}/{total}] 进度: {idx/total*100:.1f}%")
            
            data = self.fetch_all_data_for_date(trade_date)
            all_data.append(data)
            
            # 进度提示
            if idx % 5 == 0:
                print(f"\n{'='*60}")
                print(f"已完成 {idx}/{total} 个交易日")
                print(f"{'='*60}\n")
        
        print(f"\n🎉 批量数据获取完成！共{len(all_data)}个交易日")
        return all_data


def test_connection():
    """测试tushare连接"""
    try:
        fetcher = DataFetcher()
        
        # 测试获取最近一个交易日
        today = datetime.now().strftime('%Y%m%d')
        yesterday = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        
        trade_dates = fetcher.get_trade_cal(yesterday, today)
        
        if trade_dates:
            print(f"\n📅 最近的交易日: {trade_dates[-1]}")
            
            # 测试获取数据
            test_date = trade_dates[-1]
            data = fetcher.fetch_all_data_for_date(test_date)
            
            print("\n✅ 连接测试成功！")
            print(f"日线数据: {len(data['daily'])}条")
            print(f"涨停数据: {len(data['limit_up'])}条")
            print(f"跌停数据: {len(data['limit_down'])}条")
            print(f"炸板数据: {len(data['break_board'])}条")
            return True
        else:
            print("❌ 未找到交易日")
            return False
            
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False


if __name__ == '__main__':
    # 测试连接
    test_connection()
