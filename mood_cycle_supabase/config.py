"""
配置文件
存储配置信息
"""
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Tushare配置
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', '7fc6871c1e92fad3b8d5c9fa6dcaa0f3a47f94adb9d10c5d87d021e2')

# Supabase配置
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# 初始数据范围
INIT_START_DATE = '20260101'

# 市场配置
# 排除ST、退市股票
EXCLUDE_ST = True

# API调用配置
API_DELAY = 0.3

# Web服务配置
WEB_HOST = '0.0.0.0'
WEB_PORT = 5000
DEBUG_MODE = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'

# 颜色标记阈值配置（用于前端展示）
COLOR_THRESHOLDS = {
    'advance_1to2': {'good': 40, 'bad': 20},
    'advance_2to3': {'good': 40, 'bad': 20},
    'advance_3to4': {'good': 40, 'bad': 20},
    'advance_3plus': {'good': 35, 'bad': 15},
    'break_rate': {'good': 20, 'bad': 40, 'reverse': True},
    'first_red_rate': {'good': 60, 'bad': 40},
    'second_red_rate': {'good': 60, 'bad': 40},
    'third_red_rate': {'good': 60, 'bad': 40},
    'first_premium': {'good': 3, 'bad': -1},
    'second_premium': {'good': 3, 'bad': -1},
    'third_premium': {'good': 3, 'bad': -1},
}
