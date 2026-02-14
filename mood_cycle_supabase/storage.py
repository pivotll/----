"""
数据存储模块 (Supabase 版本)
负责与 Supabase 数据库交互
"""

import pandas as pd
from supabase import create_client, Client
import config
from datetime import datetime
from typing import List, Dict, Optional
import os

class DataStorage:
    """Supabase 数据存储管理器"""
    
    def __init__(self):
        """初始化 Supabase 客户端"""
        self.url = config.SUPABASE_URL
        self.key = config.SUPABASE_KEY
        
        if not self.url or not self.key:
            print("[警告] 未配置 SUPABASE_URL 或 SUPABASE_KEY，数据库功能将不可用")
            self.supabase = None
        else:
            self.supabase: Client = create_client(self.url, self.key)
    
    def save_emotion_indicators(self, indicators_list: List[dict]):
        """
        保存情绪指标到 Supabase
        """
        if not indicators_list or not self.supabase:
            return
        
        print("\n[保存] 开始保存情绪指标到 Supabase...")
        
        # 批量插入或更新 (Upsert)
        # Supabase 的 upsert 需要指定 conflict column，这里假设是 trade_date
        try:
            # 确保数据格式正确，特别是日期
            formatted_data = []
            for item in indicators_list:
                # 复制一份以防修改原数据
                record = item.copy()
                # 确保 trade_date 是 YYYY-MM-DD 格式 (数据库通常用 date 类型)
                if 'trade_date' in record:
                    # 如果是 '20260101' 格式，转为 '2026-01-01'
                    if isinstance(record['trade_date'], str) and len(record['trade_date']) == 8:
                        record['trade_date'] = f"{record['trade_date'][:4]}-{record['trade_date'][4:6]}-{record['trade_date'][6:]}"
                formatted_data.append(record)

            # 分批处理，避免一次请求过大
            batch_size = 100
            for i in range(0, len(formatted_data), batch_size):
                batch = formatted_data[i:i+batch_size]
                self.supabase.table('emotion_cycle').upsert(batch, on_conflict='trade_date').execute()
            
            print(f"  [OK] 成功保存 {len(formatted_data)} 条记录")
            
        except Exception as e:
            print(f"[错误] 保存到 Supabase 失败: {e}")

    def load_emotion_indicators(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        读取情绪指标数据
        Args:
            start_date: 开始日期 YYYY-MM-DD 或 YYYYMMDD
            end_date: 结束日期 YYYY-MM-DD 或 YYYYMMDD
        """
        if not self.supabase:
            return pd.DataFrame()
        
        try:
            query = self.supabase.table('emotion_cycle').select("*")
            
            if start_date:
                # 统一格式化为 YYYY-MM-DD
                if len(start_date) == 8:
                    start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
                query = query.gte('trade_date', start_date)
            
            if end_date:
                if len(end_date) == 8:
                    end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
                query = query.lte('trade_date', end_date)
            
            # 执行查询
            response = query.execute()
            data = response.data
            
            if not data:
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            
            # 转换 trade_date 为 datetime 对象以便后续处理
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            return df
            
        except Exception as e:
            print(f"[错误] 从 Supabase 读取失败: {e}")
            return pd.DataFrame()

    def get_latest_date(self) -> Optional[str]:
        """
        获取数据库中最新的交易日期
        
        Returns:
            日期字符串（YYYYMMDD格式）或None
        """
        if not self.supabase:
            return None
        
        try:
            # 获取最晚日期
            max_res = self.supabase.table('emotion_cycle').select('trade_date').order('trade_date', desc=True).limit(1).execute()
            
            if max_res.data:
                # Supabase returns YYYY-MM-DD, convert to YYYYMMDD
                return max_res.data[0]['trade_date'].replace('-', '')
            return None
        except Exception as e:
            print(f"[错误] 获取最新日期失败: {e}")
            return None

    def log_update_run(self, mode: str, start_date: Optional[str], end_date: Optional[str], days_count: Optional[int], status: str = 'success', message: Optional[str] = None):
        if not self.supabase:
            return

        try:
            payload = {
                'mode': mode,
                'start_date': start_date,
                'end_date': end_date,
                'days_count': days_count,
                'status': status,
                'message': message,
            }
            self.supabase.table('update_log').insert(payload).execute()
        except Exception as e:
            print(f"[错误] 写入 update_log 失败: {e}")

    def get_last_update_run(self) -> Optional[dict]:
        if not self.supabase:
            return None

        try:
            res = (
                self.supabase.table('update_log')
                .select('run_at,mode,start_date,end_date,days_count,status,message')
                .order('run_at', desc=True)
                .limit(1)
                .execute()
            )
            if res.data:
                return res.data[0]
            return None
        except Exception as e:
            print(f"[错误] 读取 update_log 失败: {e}")
            return None

    def get_data_date_range(self) -> tuple:
        """获取数据的日期范围"""
        if not self.supabase:
            return None, None
            
        try:
            # 获取最早日期
            min_res = self.supabase.table('emotion_cycle').select('trade_date').order('trade_date', desc=False).limit(1).execute()
            # 获取最晚日期
            max_res = self.supabase.table('emotion_cycle').select('trade_date').order('trade_date', desc=True).limit(1).execute()
            
            min_date = min_res.data[0]['trade_date'].replace('-', '') if min_res.data else None
            max_date = max_res.data[0]['trade_date'].replace('-', '') if max_res.data else None
            
            return min_date, max_date
        except Exception as e:
            print(f"[错误] 获取日期范围失败: {e}")
            return None, None

    # 以下方法暂时保留空实现或简化实现，因为 Vercel 部署主要关注 emotion_cycle 的展示
    # 如果需要完整功能（如 raw data 存储），需要在 Supabase 建更多表
    
    def save_raw_data(self, all_data: List[dict]):
        """
        (简化版) 仅打印日志，实际生产环境建议存到 Supabase Storage 或其他表
        """
        print("[提示] Supabase 版本暂不存储 raw_data (daily/limit/basic) 以节省数据库空间，仅存储 emotion_cycle 指标。")
        pass

    def load_limit_data(self, start_date: str, end_date: str, limit_type: str = None) -> pd.DataFrame:
        """暂不支持从 Supabase 读取详细涨跌停数据"""
        return pd.DataFrame()
    
    def load_daily_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """暂不支持从 Supabase 读取详细日线数据"""
        return pd.DataFrame()

    def export_to_excel(self, df: pd.DataFrame, output_file: str):
        """保持原有的 Excel 导出逻辑 (在内存/临时文件处理)"""
        try:
            # 确保目录存在 (Vercel 中只能写 /tmp，但这里是 output_file 参数决定)
            # 如果是 Vercel 环境，output_file 应该是 /tmp/...
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            
            df.to_excel(output_file, index=False, engine='openpyxl')
            print(f"[完成] 数据已导出到: {output_file}")
        except Exception as e:
            print(f"[错误] 导出Excel失败: {e}")
