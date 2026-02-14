"""
数据存储模块
负责Parquet文件的读写和管理
"""

import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Optional
import config


class DataStorage:
    """Parquet数据存储管理器"""
    
    def __init__(self):
        """初始化存储目录"""
        self._ensure_directories()
    
    def _ensure_directories(self):
        """确保数据目录存在"""
        os.makedirs(config.RAW_DATA_DIR, exist_ok=True)
        os.makedirs(config.PROCESSED_DATA_DIR, exist_ok=True)
    
    def save_raw_data(self, all_data: List[dict]):
        """
        保存原始数据到Parquet文件
        
        Args:
            all_data: 数据列表，每个元素是一个日期的所有数据
        """
        print("\n[保存] 开始保存原始数据...")
        
        # 合并所有日线数据和涨跌停数据
        daily_list = []
        limit_data_list = []
        daily_basic_list = []
        
        for data in all_data:
            if not data['daily'].empty:
                daily_list.append(data['daily'])
            if not data['limit_data'].empty:
                limit_data_list.append(data['limit_data'])
            if not data['daily_basic'].empty:
                daily_basic_list.append(data['daily_basic'])
        
        # 保存日线数据
        if daily_list:
            daily_df = pd.concat(daily_list, ignore_index=True)
            self._append_or_create(config.DAILY_DATA_FILE, daily_df)
            print(f"  [OK] 日线数据: {len(daily_df)}条")
        
        # 保存涨跌停数据（包含limit字段）
        if limit_data_list:
            all_limit_df = pd.concat(limit_data_list, ignore_index=True)
            self._append_or_create(config.LIMIT_LIST_FILE, all_limit_df)
            
            # 统计各类型
            limit_u = len(all_limit_df[all_limit_df['limit'] == 'U']) if 'limit' in all_limit_df.columns else 0
            limit_d = len(all_limit_df[all_limit_df['limit'] == 'D']) if 'limit' in all_limit_df.columns else 0
            limit_z = len(all_limit_df[all_limit_df['limit'] == 'Z']) if 'limit' in all_limit_df.columns else 0
            print(f"  [OK] 涨跌停数据: {len(all_limit_df)}条 (涨停{limit_u} 跌停{limit_d} 炸板{limit_z})")
        
        # 保存基础指标数据
        if daily_basic_list:
            basic_df = pd.concat(daily_basic_list, ignore_index=True)
            self._append_or_create(config.DAILY_BASIC_FILE, basic_df)
            print(f"  [OK] 基础指标数据: {len(basic_df)}条")
        
        print("[完成] 原始数据保存完成\n")
    
    def save_emotion_indicators(self, indicators_list: List[dict]):
        """
        保存情绪指标到Parquet文件
        
        Args:
            indicators_list: 指标列表
        """
        if not indicators_list:
            print("[警告] 无指标数据可保存")
            return
        
        print("\n[保存] 开始保存情绪指标...")
        
        # 转换为DataFrame
        df = pd.DataFrame(indicators_list)
        
        # 确保trade_date是日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        
        # 追加或创建文件
        self._append_or_create(config.EMOTION_CYCLE_FILE, df)
        
        print(f"  [OK] 情绪指标: {len(df)}条记录")
        print("[完成] 情绪指标保存完成\n")
    
    def _append_or_create(self, file_path: str, new_df: pd.DataFrame):
        """
        追加数据到Parquet文件，如果文件不存在则创建
        
        Args:
            file_path: 文件路径
            new_df: 新数据
        """
        if os.path.exists(file_path):
            # 读取现有数据
            existing_df = pd.read_parquet(file_path)
            
            # 合并数据
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # 去重（基于trade_date和ts_code）
            if 'ts_code' in combined_df.columns and 'trade_date' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(
                    subset=['trade_date', 'ts_code'], 
                    keep='last'
                )
            elif 'trade_date' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(
                    subset=['trade_date'], 
                    keep='last'
                )
            
            # 排序
            if 'trade_date' in combined_df.columns:
                combined_df = combined_df.sort_values('trade_date')
            
            # 保存
            combined_df.to_parquet(file_path, index=False, compression='snappy')
        else:
            # 直接创建新文件
            new_df.to_parquet(file_path, index=False, compression='snappy')
    
    def load_emotion_indicators(self, start_date: str = None, 
                                end_date: str = None) -> pd.DataFrame:
        """
        读取情绪指标数据
        
        Args:
            start_date: 开始日期，格式：YYYY-MM-DD 或 YYYYMMDD
            end_date: 结束日期，格式：YYYY-MM-DD 或 YYYYMMDD
            
        Returns:
            DataFrame
        """
        if not os.path.exists(config.EMOTION_CYCLE_FILE):
            print("[警告] 情绪指标文件不存在")
            return pd.DataFrame()
        
        df = pd.read_parquet(config.EMOTION_CYCLE_FILE)
        
        # 日期过滤
        if start_date:
            start_date = pd.to_datetime(start_date)
            df = df[df['trade_date'] >= start_date]
        
        if end_date:
            end_date = pd.to_datetime(end_date)
            df = df[df['trade_date'] <= end_date]
        
        return df
    
    def get_latest_date(self) -> Optional[str]:
        """
        获取数据库中最新的交易日期
        
        Returns:
            日期字符串（YYYYMMDD格式）或None
        """
        if not os.path.exists(config.EMOTION_CYCLE_FILE):
            return None
        
        df = pd.read_parquet(config.EMOTION_CYCLE_FILE)
        
        if df.empty:
            return None
        
        latest_date = df['trade_date'].max()
        return latest_date.strftime('%Y%m%d')
    
    def get_data_date_range(self) -> tuple:
        """
        获取数据的日期范围
        
        Returns:
            (最早日期, 最晚日期) 元组，格式：YYYYMMDD
        """
        if not os.path.exists(config.EMOTION_CYCLE_FILE):
            return None, None
        
        df = pd.read_parquet(config.EMOTION_CYCLE_FILE)
        
        if df.empty:
            return None, None
        
        min_date = df['trade_date'].min().strftime('%Y%m%d')
        max_date = df['trade_date'].max().strftime('%Y%m%d')
        
        return min_date, max_date
    
    def load_limit_data(self, start_date: str, end_date: str, limit_type: str = None) -> pd.DataFrame:
        """
        读取指定日期范围的涨跌停数据
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            limit_type: 类型过滤，U=涨停, D=跌停, Z=炸板, None=全部
            
        Returns:
            DataFrame
        """
        if not os.path.exists(config.LIMIT_LIST_FILE):
            return pd.DataFrame()
        
        df = pd.read_parquet(config.LIMIT_LIST_FILE)
        
        # 日期过滤
        df = df[
            (df['trade_date'] >= start_date) & 
            (df['trade_date'] <= end_date)
        ]
        
        # 类型过滤
        if limit_type and 'limit' in df.columns:
            df = df[df['limit'] == limit_type]
        
        return df
    
    def load_daily_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        读取指定日期范围的日线数据
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            
        Returns:
            DataFrame
        """
        if not os.path.exists(config.DAILY_DATA_FILE):
            return pd.DataFrame()
        
        df = pd.read_parquet(config.DAILY_DATA_FILE)
        
        # 日期过滤
        df = df[
            (df['trade_date'] >= start_date) & 
            (df['trade_date'] <= end_date)
        ]
        
        return df
    
    def export_to_excel(self, df: pd.DataFrame, output_file: str):
        """
        导出数据到Excel
        
        Args:
            df: DataFrame
            output_file: 输出文件路径
        """
        try:
            # 格式化日期列
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            
            df.to_excel(output_file, index=False, engine='openpyxl')
            print(f"[完成] 数据已导出到: {output_file}")
            
        except Exception as e:
            print(f"[错误] 导出Excel失败: {e}")


def test_storage():
    """测试存储模块"""
    print("=" * 60)
    print("测试存储模块")
    print("=" * 60)
    
    storage = DataStorage()
    
    # 测试保存情绪指标
    test_indicators = [
        {
            'trade_date': '20260120',
            'up_count': 100,
            'down_count': 50,
            'limit_up_count': 10,
            'advance_1to2': 45.5
        },
        {
            'trade_date': '20260121',
            'up_count': 120,
            'down_count': 40,
            'limit_up_count': 15,
            'advance_1to2': 50.0
        }
    ]
    
    storage.save_emotion_indicators(test_indicators)
    
    # 测试读取
    df = storage.load_emotion_indicators()
    print(f"\n读取数据: {len(df)}条")
    print(df.head())
    
    # 测试获取最新日期
    latest = storage.get_latest_date()
    print(f"\n最新日期: {latest}")
    
    # 测试日期范围
    min_date, max_date = storage.get_data_date_range()
    print(f"日期范围: {min_date} ~ {max_date}")
    
    print("\n[完成] 测试完成")


if __name__ == '__main__':
    test_storage()
