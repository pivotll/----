"""
Flask Web服务
提供API接口和页面展示
"""

# 设置编码
import utils
utils.setup_encoding()

from flask import Flask, render_template, jsonify, request, send_file, Response
from flask_cors import CORS
import pandas as pd
import json
from datetime import datetime
import os
import config
from storage import DataStorage

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 自定义JSON编码器处理NaN
class NanToNullEncoder(json.JSONEncoder):
    def encode(self, obj):
        if isinstance(obj, float):
            if pd.isna(obj):
                return 'null'
        return super().encode(obj)
    
    def iterencode(self, obj, _one_shot=False):
        """自定义iterencode以处理NaN"""
        for chunk in super().iterencode(obj, _one_shot):
            # 替换NaN为null
            chunk = chunk.replace('NaN', 'null')
            yield chunk

storage = DataStorage()


@app.route('/')
def index():
    """首页"""
    return render_template('index.html')


@app.route('/api/data')
def get_data():
    """
    获取情绪数据API
    参数：
        start_date: 开始日期（可选），格式：YYYY-MM-DD
        end_date: 结束日期（可选），格式：YYYY-MM-DD
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        # 读取数据
        df = storage.load_emotion_indicators(start_date, end_date)
        
        if df.empty:
            return jsonify({
                'success': False,
                'message': '无数据'
            })
        
        # 排序（最新日期在前：日期从大到小）
        df = df.sort_values('trade_date', ascending=False)

        # 转换日期格式
        df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
        
        # 转换为字典列表
        data = df.to_dict('records')
        
        # 使用自定义编码器处理NaN
        result = {
            'success': True,
            'data': data,
            'count': len(data)
        }
        
        return Response(
            json.dumps(result, cls=NanToNullEncoder, ensure_ascii=False),
            mimetype='application/json'
        )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取数据失败: {str(e)}'
        })


@app.route('/api/stats')
def get_stats():
    """获取数据统计信息"""
    try:
        min_date, max_date = storage.get_data_date_range()
        
        if min_date is None:
            return jsonify({
                'success': False,
                'message': '暂无数据'
            })
        
        df = storage.load_emotion_indicators()

        last_run = storage.get_last_update_run() or {}
        
        # 格式化日期
        min_date_str = datetime.strptime(min_date, '%Y%m%d').strftime('%Y-%m-%d')
        max_date_str = datetime.strptime(max_date, '%Y%m%d').strftime('%Y-%m-%d')
        
        return jsonify({
            'success': True,
            'min_date': min_date_str,
            'max_date': max_date_str,
            'total_days': len(df),
            'last_update_at': last_run.get('run_at'),
            'last_update_mode': last_run.get('mode'),
            'last_update_days': last_run.get('days_count'),
            'last_update_status': last_run.get('status'),
            'last_update_message': last_run.get('message'),
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取统计信息失败: {str(e)}'
        })


@app.route('/api/export')
def export_excel():
    """导出Excel"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        # 读取数据
        df = storage.load_emotion_indicators(start_date, end_date)
        
        if df.empty:
            return jsonify({
                'success': False,
                'message': '无数据可导出'
            })
        
        # 排序（最新日期在前：日期从大到小）
        df = df.sort_values('trade_date', ascending=False)
        
        # 生成临时文件
        output_file = os.path.join('data', f'情绪周期表_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
        storage.export_to_excel(df, output_file)
        
        # 发送文件
        return send_file(
            output_file,
            as_attachment=True,
            download_name=f'情绪周期表_{datetime.now().strftime("%Y%m%d")}.xlsx'
        )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'导出失败: {str(e)}'
        })


@app.route('/api/color_config')
def get_color_config():
    """获取颜色标记配置"""
    return jsonify({
        'success': True,
        'config': config.COLOR_THRESHOLDS
    })



if __name__ == '__main__':
    print("=" * 70)
    print("🌐 启动市场情绪周期监控系统")
    print("=" * 70)
    print(f"访问地址: http://{config.WEB_HOST}:{config.WEB_PORT}")
    print("=" * 70)
    
    app.run(
        host=config.WEB_HOST,
        port=config.WEB_PORT,
        debug=config.DEBUG_MODE
    )
