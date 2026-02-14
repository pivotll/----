@echo off
REM 启动 市场情绪周期监控系统

REM 切换到项目目录
cd /d "D:\_TradeCode\复盘工具\情绪周期表"

echo ==============================================
echo 启动 市场情绪周期监控系统
echo 项目目录: %CD%
echo Python 路径: %PYTHON%
echo ==============================================

REM 启动 Flask Web 服务
python app.py

echo.
echo 服务已退出，如需再次启动请重新双击本脚本。
pause

