"""数据验证脚本"""
import utils
utils.setup_encoding()

from storage import DataStorage

storage = DataStorage()
df = storage.load_emotion_indicators()

# 查看2026-01-26的数据
data_26 = df[df['trade_date'] == '2026-01-26'].iloc[0]

print('=' * 70)
print('2026-01-26 完整指标验证')
print('=' * 70)

print('\n【市场概况】')
print(f'  涨停: {int(data_26["limit_up_count"])}只 | 跌停: {int(data_26["limit_down_count"])}只 | 炸板: {int(data_26["break_count"])}只')
print(f'  炸板率: {data_26["break_rate"]:.1f}%')
print(f'  首板: {int(data_26["first_board"])}只 | 二板: {int(data_26["second_board"])}只 | 三板: {int(data_26["third_board"])}只')
print(f'  三板以上: {int(data_26["above_third"])}只 | 最高板: {int(data_26["max_board"])}板')

print('\n【晋级率（修正后）】')
print(f'  1进2（昨日首板今日任意涨停）: {data_26["advance_1to2"]:.1f}%')
print(f'  2进3（昨日二板今日任意涨停）: {data_26["advance_2to3"]:.1f}%')
print(f'  3板以上（昨日3板+今日任意涨停）: {data_26["advance_3plus"]:.1f}%')

print('\n【今日涨停板表现】')
print(f'  首板红盘率（开盘≥昨收）: {data_26["first_red_rate"]:.1f}%')
print(f'  首板溢价（平均涨幅）: {data_26["first_premium"]:.2f}%')
print(f'  二板红盘率: {data_26["second_red_rate"]:.1f}%')
print(f'  二板溢价: {data_26["second_premium"]:.2f}%')

print('\n【昨日涨停今日表现（新增）】')
print(f'  昨日首板今日红盘率: {data_26["yesterday_first_red_rate"]:.1f}%')
print(f'  昨日首板今日溢价: {data_26["yesterday_first_premium"]:.2f}%')
print(f'  昨日二板今日红盘率: {data_26["yesterday_second_red_rate"]:.1f}%')
print(f'  昨日二板今日溢价: {data_26["yesterday_second_premium"]:.2f}%')

print('\n【数据对比（修正前 vs 修正后）】')
print('✅ 涨停数量：现在包含实际封板的涨停（不含炸板）')
print('✅ 红盘率：从"close≥open"改为"open≥pre_close"，更合理')
print('✅ 晋级率：从"昨日N板→今日N+1板"改为"昨日N板→今日任意涨停"，更准确')
print('✅ 新增：昨日涨停今日表现，可追踪连板股票次日走势')

print('\n' + '=' * 70)
print('验证完成！')
print('=' * 70)
