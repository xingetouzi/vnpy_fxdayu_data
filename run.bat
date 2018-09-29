@echo off

REM conda 虚拟环境
REM call activate vnpy

set PYTHONPATH=%cd%

REM 创建biance数据索引
echo create binance index
python binance/binance.py create

REM 更新biance数据
echo update binance data
python binance/binance.py update publish

REM 创建jaqs一分钟数据索引
echo create jaqs m1 index
python jqdata/jqdata.py create

REM 更新jaqs一分钟数据
echo update jaqs m1 data
python jqdata/jqdata.py update publish

REM 创建oanda数据索引
echo create oanda index
python oanda/m1.py create

REM 更新oanda数据
echo update oanda data
python oanda/m1.py update publish

pause