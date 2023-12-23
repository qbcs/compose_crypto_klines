# 加密货币K线生成工具

这个工具可以使用加密货币的1min周期K线来组合任意周期N minutes、任意起点(0 ~ N-1)的K线

使用方法

* 使用币安提供的下载脚本下载原始1min K线数据
* 
   git clone git@github.com:binance/binance-public-data.git
  
   cd binance-public-data/python
  
   python download-kline.py -s BTCUSDT ETHUSDT BNBUSDT SOLUSDT DOGEUSDT XRPUSDT -startDate 2017-01-01 -endDate 2023-12-31 -folder /Volumes/HFT/Crypto/binance-data/official-python-download-1m -skip-daily 1 -t spot -i 1m
  
* 使用1min K线数据转换成30min K线数据（需提前安装go并编译此程序）

   cd compose_crypto_klines
  
   go build
  
   ./compose_crypto_klines -folder=/Volumes/HFT/Crypto/binance-data/official-python-download-1m -download-date-range=2017-01-01_2023-12-31 -tickers=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,DOGEUSDT,XRPUSDT -interval=30
