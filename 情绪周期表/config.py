"""
配置文件
存储tushare token和其他系统配置
"""

# Tushare配置
TUSHARE_TOKEN = '7fc6871c1e92fad3b8d5c9fa6dcaa0f3a47f94adb9d10c5d87d021e2'

# 数据路径配置
DATA_DIR = 'data'
RAW_DATA_DIR = 'data/raw'
PROCESSED_DATA_DIR = 'data/processed'

# 数据文件路径
DAILY_DATA_FILE = 'data/raw/daily.parquet'
LIMIT_LIST_FILE = 'data/raw/limit_list.parquet'
DAILY_BASIC_FILE = 'data/raw/daily_basic.parquet'
EMOTION_CYCLE_FILE = 'data/processed/emotion_cycle.parquet'

# 初始数据范围
INIT_START_DATE = '20260101'  # 2026-01-01

# 市场配置
# 排除ST、退市股票
EXCLUDE_ST = True

# API调用配置
# 为避免频繁调用，建议设置合理的延迟
API_DELAY = 0.3  # 每次API调用间隔（秒）

# Web服务配置
WEB_HOST = '127.0.0.1'
WEB_PORT = 5000
DEBUG_MODE = True

# 颜色标记阈值配置（用于前端展示）
COLOR_THRESHOLDS = {
    # 晋级率：>40%为好，<20%为差
    'advance_1to2': {'good': 40, 'bad': 20},
    'advance_2to3': {'good': 40, 'bad': 20},
    'advance_3to4': {'good': 40, 'bad': 20},
    'advance_3plus': {'good': 35, 'bad': 15},
    
    # 炸板率：<20%为好，>40%为差
    'break_rate': {'good': 20, 'bad': 40, 'reverse': True},
    
    # 红盘率：>60%为好，<40%为差
    'first_red_rate': {'good': 60, 'bad': 40},
    'second_red_rate': {'good': 60, 'bad': 40},
    'third_red_rate': {'good': 60, 'bad': 40},
    
    # 溢价：>3%为好，<-1%为差
    'first_premium': {'good': 3, 'bad': -1},
    'second_premium': {'good': 3, 'bad': -1},
    'third_premium': {'good': 3, 'bad': -1},
}
