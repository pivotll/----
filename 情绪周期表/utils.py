"""
工具函数模块
"""

import sys
import io


def setup_encoding():
    """设置控制台编码为UTF-8"""
    if sys.stdout.encoding != 'utf-8':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except AttributeError:
            # Python 3.6及以下版本
            if hasattr(sys.stdout, 'buffer'):
                sys.stdout = io.TextIOWrapper(
                    sys.stdout.buffer, 
                    encoding='utf-8',
                    errors='replace',
                    line_buffering=True
                )
