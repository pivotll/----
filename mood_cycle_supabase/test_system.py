"""
系统测试脚本
验证数据准确性和功能完整性
"""

import utils
utils.setup_encoding()

from storage import DataStorage
from data_fetcher import DataFetcher
import pandas as pd

def test_data_accuracy():
    """测试数据准确性"""
    print("=" * 70)
    print("📊 数据准确性测试")
    print("=" * 70)
    
    storage = DataStorage()
    
    # 1. 检查数据完整性
    min_date, max_date = storage.get_data_date_range()
    print(f"\n✓ 数据范围: {min_date} ~ {max_date}")
    
    df = storage.load_emotion_indicators()
    print(f"✓ 交易日数: {len(df)}天")
    
    # 2. 检查关键指标是否有空值
    print("\n📈 关键指标完整性检查:")
    key_columns = ['limit_up_count', 'first_board', 'max_board', 'break_rate']
    for col in key_columns:
        null_count = df[col].isnull().sum()
        print(f"  {col}: {'✓ 无空值' if null_count == 0 else f'⚠️  有{null_count}个空值'}")
    
    # 3. 检查数值合理性
    print("\n🔢 数值合理性检查:")
    latest = df.iloc[-1]
    
    # 涨停数应该大于0
    if latest['limit_up_count'] > 0:
        print(f"  ✓ 涨停数: {latest['limit_up_count']}只")
    else:
        print(f"  ⚠️  涨停数异常: {latest['limit_up_count']}")
    
    # 最高板应该大于0
    if latest['max_board'] > 0:
        print(f"  ✓ 最高板: {latest['max_board']}板")
    else:
        print(f"  ⚠️  最高板异常: {latest['max_board']}")
    
    # 炸板率应该在0-100之间
    if 0 <= latest['break_rate'] <= 100:
        print(f"  ✓ 炸板率: {latest['break_rate']}%")
    else:
        print(f"  ⚠️  炸板率异常: {latest['break_rate']}")
    
    # 4. 显示最新数据
    print("\n📅 最新3天数据:")
    display_cols = ['trade_date', 'limit_up_count', 'max_board', 'first_red_rate', 
                    'first_premium', 'break_rate']
    print(df.tail(3)[display_cols].to_string(index=False))
    
    print("\n✅ 数据准确性测试完成")
    return True


def test_api_connection():
    """测试API连接"""
    print("\n" + "=" * 70)
    print("🔌 API连接测试")
    print("=" * 70)
    
    try:
        fetcher = DataFetcher()
        print("✓ Tushare连接成功")
        return True
    except Exception as e:
        print(f"✗ Tushare连接失败: {e}")
        return False


def test_storage():
    """测试存储功能"""
    print("\n" + "=" * 70)
    print("💾 存储功能测试")
    print("=" * 70)
    
    storage = DataStorage()
    
    # 测试读取
    df = storage.load_emotion_indicators()
    if not df.empty:
        print(f"✓ 数据读取成功: {len(df)}条记录")
    else:
        print("✗ 数据读取失败")
        return False
    
    # 测试日期过滤
    df_filtered = storage.load_emotion_indicators(start_date='2026-01-10')
    if not df_filtered.empty:
        print(f"✓ 日期过滤成功: {len(df_filtered)}条记录")
    else:
        print("⚠️  日期过滤后无数据")
    
    print("✅ 存储功能测试完成")
    return True


def test_web_api():
    """测试Web API"""
    print("\n" + "=" * 70)
    print("🌐 Web API测试")
    print("=" * 70)
    
    try:
        import requests
        
        # 测试数据接口
        response = requests.get('http://127.0.0.1:5000/api/data', timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data['success']:
                print(f"✓ 数据接口正常: 返回{data['count']}条记录")
            else:
                print(f"✗ 数据接口异常: {data.get('message', '未知错误')}")
        else:
            print(f"✗ 数据接口HTTP错误: {response.status_code}")
        
        # 测试统计接口
        response = requests.get('http://127.0.0.1:5000/api/stats', timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data['success']:
                print(f"✓ 统计接口正常: {data['min_date']} ~ {data['max_date']}")
            else:
                print(f"✗ 统计接口异常: {data.get('message', '未知错误')}")
        else:
            print(f"✗ 统计接口HTTP错误: {response.status_code}")
        
        print("✅ Web API测试完成")
        return True
        
    except requests.exceptions.ConnectionError:
        print("⚠️  Web服务未启动，跳过API测试")
        print("   提示：请运行 python app.py 启动服务后再测试")
        return False
    except Exception as e:
        print(f"✗ Web API测试失败: {e}")
        return False


def main():
    """主测试流程"""
    print("\n" + "=" * 70)
    print("🧪 市场情绪周期监控系统 - 完整测试")
    print("=" * 70)
    
    results = {
        'API连接': test_api_connection(),
        '存储功能': test_storage(),
        '数据准确性': test_data_accuracy(),
        'Web API': test_web_api()
    }
    
    # 测试总结
    print("\n" + "=" * 70)
    print("📋 测试总结")
    print("=" * 70)
    
    passed = sum(results.values())
    total = len(results)
    
    for name, result in results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {name}: {status}")
    
    print("\n" + "-" * 70)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("\n🎉 所有测试通过！系统运行正常")
    elif passed >= total - 1:
        print("\n⚠️  大部分测试通过，系统基本可用")
    else:
        print("\n❌ 多项测试失败，请检查系统配置")
    
    print("=" * 70)


if __name__ == '__main__':
    main()
