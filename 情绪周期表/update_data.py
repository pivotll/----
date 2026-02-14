"""
数据更新脚本
负责初始化数据和增量更新
"""

# 必须在最开始设置编码
import utils
utils.setup_encoding()

import argparse
from datetime import datetime, timedelta
from data_fetcher import DataFetcher
from indicators import IndicatorCalculator
from storage import DataStorage
import config


class DataUpdater:
    """数据更新器"""
    
    def __init__(self):
        """初始化"""
        self.fetcher = DataFetcher()
        self.calculator = IndicatorCalculator()
        self.storage = DataStorage()
    
    def initialize_data(self, start_date: str = None):
        """
        初始化数据
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD，默认从config中获取
        """
        if start_date is None:
            start_date = config.INIT_START_DATE
        
        # 结束日期为今天
        end_date = datetime.now().strftime('%Y%m%d')
        
        print("=" * 70)
        print("📦 数据初始化")
        print("=" * 70)
        print(f"起始日期: {start_date}")
        print(f"结束日期: {end_date}")
        print("=" * 70)
        
        # 批量获取数据
        all_data = self.fetcher.fetch_batch_data(start_date, end_date)
        
        if not all_data:
            print("❌ 未获取到数据")
            return
        
        # 保存原始数据
        self.storage.save_raw_data(all_data)
        
        # 计算指标
        print("\n🔢 开始计算指标...")
        indicators_list = []
        
        for i, data in enumerate(all_data):
            # 获取前一日数据（用于计算晋级率）
            prev_data = all_data[i-1] if i > 0 else None
            
            # 计算指标
            indicators = self.calculator.calculate_indicators(data, prev_data)
            indicators_list.append(indicators)
            
            if (i + 1) % 10 == 0:
                print(f"  已计算 {i+1}/{len(all_data)} 个交易日")
        
        print(f"✅ 指标计算完成，共{len(indicators_list)}条")
        
        # 保存指标
        self.storage.save_emotion_indicators(indicators_list)
        
        print("\n" + "=" * 70)
        print("🎉 数据初始化完成！")
        print("=" * 70)
        
        # 显示数据摘要
        self._show_data_summary()
    
    def update_incremental(self):
        """增量更新数据"""
        print("=" * 70)
        print("🔄 增量数据更新")
        print("=" * 70)
        
        # 获取本地最新日期
        latest_date = self.storage.get_latest_date()
        
        if latest_date is None:
            print("⚠️  本地无数据，请先执行初始化：python update_data.py --init")
            return
        
        print(f"本地最新日期: {latest_date}")
        
        # 计算需要更新的日期范围
        start_date = (datetime.strptime(latest_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')
        
        print(f"检查更新范围: {start_date} ~ {end_date}")
        
        # 获取交易日历
        trade_dates = self.fetcher.get_trade_cal(start_date, end_date)
        
        if not trade_dates:
            print("✅ 数据已是最新，无需更新")
            return
        
        print(f"📅 发现{len(trade_dates)}个缺失交易日，开始更新...")
        print("=" * 70)
        
        # 获取数据
        all_data = []
        for trade_date in trade_dates:
            data = self.fetcher.fetch_all_data_for_date(trade_date)
            all_data.append(data)
        
        # 保存原始数据
        self.storage.save_raw_data(all_data)
        
        # 计算指标
        print("\n🔢 开始计算指标...")
        indicators_list = []
        
        # 获取最后一个已有数据（作为第一天的prev_data）
        # 注意：需要同时获取 limit_data 和 daily 数据，因为计算晋级率需要用到前一日的 daily
        prev_limit_df = self.storage.load_limit_data(latest_date, latest_date)
        prev_daily_df = self.storage.load_daily_data(latest_date, latest_date)
        
        if not prev_limit_df.empty or not prev_daily_df.empty:
            prev_data_first = {
                'trade_date': latest_date,
                'limit_data': prev_limit_df,
                'daily': prev_daily_df
            }
        else:
            prev_data_first = None
        
        for i, data in enumerate(all_data):
            if i == 0:
                # 第一天使用数据库中的前一日数据
                prev_data = prev_data_first
            else:
                # 后续天使用列表中的前一日数据
                prev_data = all_data[i-1]
            
            indicators = self.calculator.calculate_indicators(data, prev_data)
            indicators_list.append(indicators)
        
        print(f"✅ 指标计算完成")
        
        # 保存指标
        self.storage.save_emotion_indicators(indicators_list)
        
        print("\n" + "=" * 70)
        print(f"🎉 增量更新完成！新增{len(trade_dates)}个交易日")
        print("=" * 70)
        
        # 显示数据摘要
        self._show_data_summary()
    
    def update_custom_range(self, start_date: str, end_date: str):
        """
        更新自定义日期范围的数据
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        print("=" * 70)
        print("📦 自定义范围数据更新")
        print("=" * 70)
        print(f"日期范围: {start_date} ~ {end_date}")
        print("=" * 70)
        
        # 批量获取数据
        all_data = self.fetcher.fetch_batch_data(start_date, end_date)
        
        if not all_data:
            print("❌ 未获取到数据")
            return
        
        # 保存原始数据
        self.storage.save_raw_data(all_data)
        
        # 计算指标
        print("\n🔢 开始计算指标...")
        indicators_list = []
        
        for i, data in enumerate(all_data):
            prev_data = all_data[i-1] if i > 0 else None
            indicators = self.calculator.calculate_indicators(data, prev_data)
            indicators_list.append(indicators)
        
        print(f"✅ 指标计算完成")
        
        # 保存指标
        self.storage.save_emotion_indicators(indicators_list)
        
        print("\n" + "=" * 70)
        print("🎉 自定义范围更新完成！")
        print("=" * 70)
        
        # 显示数据摘要
        self._show_data_summary()
    
    def _show_data_summary(self):
        """显示数据摘要"""
        min_date, max_date = self.storage.get_data_date_range()
        
        if min_date and max_date:
            df = self.storage.load_emotion_indicators()
            
            print("\n📊 数据摘要:")
            print(f"  日期范围: {min_date} ~ {max_date}")
            print(f"  交易日数: {len(df)}天")
            
            # 显示最新几条数据
            print("\n  最新数据预览:")
            latest = df.tail(3)[['trade_date', 'limit_up_count', 'max_board', 
                                   'advance_1to2', 'break_rate']]
            for _, row in latest.iterrows():
                date_str = row['trade_date'].strftime('%Y-%m-%d')
                print(f"    {date_str}: 涨停{row['limit_up_count']}只, "
                      f"最高{row['max_board']}板, "
                      f"1进2:{row['advance_1to2']}%, "
                      f"炸板率:{row['break_rate']}%")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='市场情绪数据更新工具')
    parser.add_argument('--init', action='store_true', help='初始化数据（从2026-01-01至今）')
    parser.add_argument('--start', type=str, help='自定义开始日期（YYYYMMDD）')
    parser.add_argument('--end', type=str, help='自定义结束日期（YYYYMMDD）')
    
    args = parser.parse_args()
    
    updater = DataUpdater()
    
    if args.init:
        # 初始化模式
        updater.initialize_data()
    elif args.start and args.end:
        # 自定义范围模式
        updater.update_custom_range(args.start, args.end)
    else:
        # 增量更新模式
        updater.update_incremental()


if __name__ == '__main__':
    main()
