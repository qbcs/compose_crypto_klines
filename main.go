package main

import (
	"archive/zip"
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
)

// 这个工具可以使用加密货币的1min周期K线来组合任意周期N minutes、任意起点(0 ~ N-1)的K线
// 示例
//      ./compose_crypto_klines -folder=/Volumes/HFT/Crypto/binance-data/official-python-download-1m -download-date-range=2017-01-01_2023-12-31 -tickers=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,DOGEUSDT,XRPUSDT -interval=30
//      此示例会生成指定的六个交易对的30分钟周期k线，分别以0~29分钟为起点
//      上述示例中的folder和download-date-range值都是使用官方python下载数据脚本下载基础1m klines数据时使用
//      的参数(例如，对应的下载原始1min K线数据可使用命令：python download-kline.py -s BTCUSDT ETHUSDT BNBUSDT SOLUSDT DOGEUSDT XRPUSDT -startDate 2017-01-01 -endDate 2023-12-31 -folder /Volumes/HFT/Crypto/binance-data/official-python-download-1m -skip-daily 1 -t spot -i 1m)

type Kline struct {
	OpenTime                 int64
	Open                     float64
	High                     float64
	Low                      float64
	Close                    float64
	Volume                   float64
	CloseTime                int64
	QuoteAssetVolume         float64
	NumOfTrades              int64
	TakerBuyBaseAssetVolume  float64
	TakerBuyQuoteAssetVolume float64
	Ignore                   float64
	numOfRecords             int
}

var (
	tickersStr        string
	folder            string
	downloadDateRange string
	interval          int
)

func init() {
	flag.StringVar(&tickersStr, "tickers", "", "指定交易对，英文逗号分隔")
	flag.StringVar(&folder, "folder", "/Volumes/HFT/Crypto/binance-data/official-python-download-1m", "指定基础1min K线数据目录")
	flag.StringVar(&downloadDateRange, "download-date-range", "2017-01-01_2023-12-31", "指定使用官方python下载数据脚本下载基础数据时使用的{startDate}_{endDate}")
	flag.IntVar(&interval, "interval", 30, "指定组合成多长时间的K线")
	flag.Parse()
}

func main() {
	tickers := strings.Split(tickersStr, ",")
	folder = strings.TrimRight(folder, "/")

	if len(tickersStr) == 0 || len(folder) == 0 || interval == 0 {
		flag.PrintDefaults()

		fmt.Printf("\n使用方法\n")
		fmt.Printf("1. 下载币安原始1min K线数据\n")
		fmt.Printf("   git clone git@github.com:binance/binance-public-data.git\n")
		fmt.Printf("   cd binance-public-data/python\n")
		fmt.Printf("   python download-kline.py -s BTCUSDT ETHUSDT BNBUSDT SOLUSDT DOGEUSDT XRPUSDT -startDate 2017-01-01 -endDate 2023-12-31 -folder /Volumes/HFT/Crypto/binance-data/official-python-download-1m -skip-daily 1 -t spot -i 1m\n")
		fmt.Printf("2. 使用1min K线数据转换成30min K线数据（需提前安装go并编译此程序）\n")
		fmt.Printf("   cd compose_crypto_klines\n")
		fmt.Printf("   go build\n")
		fmt.Printf("   ./compose_crypto_klines -folder=/Volumes/HFT/Crypto/binance-data/official-python-download-1m -download-date-range=2017-01-01_2023-12-31 -tickers=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,DOGEUSDT,XRPUSDT -interval=30\n")
		return
	}

	monthRanges := getMonthRanges()

	intervalInMilliSeconds := int64(interval) * 60000

	for _, ticker := range tickers {
		var startMin int64
		for startMin = 0; startMin < int64(interval); startMin++ {
			zipReaders := make([]*zip.ReadCloser, 0, 84)
			ioReaders := make([]io.Reader, 0, 84)

			for _, month := range monthRanges {
				filePath := fmt.Sprintf("%s/data/spot/monthly/klines/%s/1m/%s/%s-1m-%s.zip", folder, ticker, downloadDateRange, ticker, month)
				if !fileExist(filePath) {
					continue
				}
				zipReader, err := zip.OpenReader(filePath)
				if err != nil {
					log.Println("zip open reader fail:", err)
					continue
				}
				zipReaders = append(zipReaders, zipReader)

				for _, f := range zipReader.File {
					r, err := f.Open()
					if err != nil {
						log.Println("zip reader file open fail.", err)
						break
					}

					ioReaders = append(ioReaders, r)
					break
				}
			}
			if len(zipReaders) == 0 {
				continue
			}
			scanner := bufio.NewScanner(io.MultiReader(ioReaders...))

			resultFilePath := fmt.Sprintf("%s/composed_klines/spot-klines-%s-%dm-%02d.csv", folder, ticker, interval, startMin)
			os.MkdirAll(path.Dir(resultFilePath), 0777)
			os.Remove(resultFilePath)
			resultFd, err := os.Create(resultFilePath)
			if err != nil {
				log.Println("创建结果文件失败", err, resultFilePath)
			}

			// 组装K线
			var baseTimestamp int64 = 1483228800000 + startMin*60000 // 基点毫秒时间戳，2017-01-01 00:00:00.000 UTC + 偏移时间量
			var lastKline *Kline = &Kline{}
			var lastKlineOpenTime int64

			for {
				if !scanner.Scan() {
					if err := scanner.Err(); err != nil {
						log.Println("逐行读取失败:", err)
					}
					break
				}
				line := strings.TrimSpace(scanner.Text())
				kline, err := parseKline(line)
				if err != nil {
					log.Println("解析单行失败", err, line)
					continue
				}
				openTime := ((kline.OpenTime-baseTimestamp)/intervalInMilliSeconds)*intervalInMilliSeconds + baseTimestamp
				closeTime := openTime + intervalInMilliSeconds - 1
				if openTime != lastKlineOpenTime {
					saveKline(lastKline, resultFd)
					cleanKline(lastKline)
					lastKlineOpenTime = openTime

					lastKline.OpenTime = openTime
					lastKline.CloseTime = closeTime
				}
				// 数据加总
				if lastKline.numOfRecords == 0 {
					lastKline.Open = kline.Open
					lastKline.High = kline.High
					lastKline.Low = kline.Low
					//lastKline.OpenTime = kline.OpenTime
				} else {
					if kline.High > lastKline.High {
						lastKline.High = kline.High
					}
					if kline.Low < lastKline.Low {
						lastKline.Low = kline.Low
					}
				}
				lastKline.Close = kline.Close
				lastKline.Volume += kline.Volume
				//lastKline.CloseTime = kline.CloseTime
				lastKline.QuoteAssetVolume += kline.QuoteAssetVolume
				lastKline.NumOfTrades += kline.NumOfTrades
				lastKline.TakerBuyBaseAssetVolume += kline.TakerBuyBaseAssetVolume
				lastKline.TakerBuyQuoteAssetVolume += kline.TakerBuyQuoteAssetVolume
				lastKline.Ignore = kline.Ignore
				lastKline.numOfRecords += 1
			}

			saveKline(lastKline, resultFd)
			cleanKline(lastKline)

			// 清理关闭文件
			for _, r := range zipReaders {
				r.Close()
			}
			resultFd.Close()
			log.Println("Created: ", resultFilePath)
		}
	}
}

func saveKline(kline *Kline, fd *os.File) {
	if kline.numOfRecords == 0 {
		return
	}

	fmt.Fprintf(fd, "%d,%.8f,%.8f,%.8f,%.8f,%.8f,%d,%.8f,%d,%.8f,%.8f,%s\n",
		kline.OpenTime,
		kline.Open,
		kline.High,
		kline.Low,
		kline.Close,
		kline.Volume,
		kline.CloseTime,
		kline.QuoteAssetVolume,
		kline.NumOfTrades,
		kline.TakerBuyBaseAssetVolume,
		kline.TakerBuyQuoteAssetVolume,
		convertIgnore(kline.Ignore),
	)
}

func convertIgnore(ignore float64) string {
	if ignore == 0 {
		return "0"
	}
	return fmt.Sprintf("%.8f", ignore)
}

func cleanKline(kline *Kline) {
	kline.OpenTime = 0
	kline.Open = 0
	kline.High = 0
	kline.Low = 0
	kline.Close = 0
	kline.Volume = 0
	kline.CloseTime = 0
	kline.QuoteAssetVolume = 0
	kline.NumOfTrades = 0
	kline.TakerBuyBaseAssetVolume = 0
	kline.TakerBuyQuoteAssetVolume = 0
	kline.Ignore = 0
	kline.numOfRecords = 0
}

func parseKline(line string) (*Kline, error) {
	if len(line) == 0 {
		return nil, errors.New("empty line.")
	}
	pieces := strings.Split(line, ",")
	if len(pieces) != 12 {
		return nil, errors.New("wrong fields")
	}
	return &Kline{
		OpenTime:                 parseInt64(pieces[0]),
		Open:                     parseFloat64(pieces[1]),
		High:                     parseFloat64(pieces[2]),
		Low:                      parseFloat64(pieces[3]),
		Close:                    parseFloat64(pieces[4]),
		Volume:                   parseFloat64(pieces[5]),
		CloseTime:                parseInt64(pieces[6]),
		QuoteAssetVolume:         parseFloat64(pieces[7]),
		NumOfTrades:              parseInt64(pieces[8]),
		TakerBuyBaseAssetVolume:  parseFloat64(pieces[9]),
		TakerBuyQuoteAssetVolume: parseFloat64(pieces[10]),
		Ignore:                   parseFloat64(pieces[11]),
	}, nil
}

func parseInt64(s string) int64 {
	r, _ := strconv.ParseInt(s, 10, 64)
	return r
}

func parseFloat64(s string) float64 {
	r, _ := strconv.ParseFloat(s, 64)
	return r
}

func fileExist(path string) bool {
	fileinfo, err := os.Stat(path)
	return err == nil && fileinfo.Mode().IsRegular()
}

func getMonthRanges() []string {
	return []string{
		"2017-01",
		"2017-02",
		"2017-03",
		"2017-04",
		"2017-05",
		"2017-06",
		"2017-07",
		"2017-08",
		"2017-09",
		"2017-10",
		"2017-11",
		"2017-12",
		"2018-01",
		"2018-02",
		"2018-03",
		"2018-04",
		"2018-05",
		"2018-06",
		"2018-07",
		"2018-08",
		"2018-09",
		"2018-10",
		"2018-11",
		"2018-12",
		"2019-01",
		"2019-02",
		"2019-03",
		"2019-04",
		"2019-05",
		"2019-06",
		"2019-07",
		"2019-08",
		"2019-09",
		"2019-10",
		"2019-11",
		"2019-12",
		"2020-01",
		"2020-02",
		"2020-03",
		"2020-04",
		"2020-05",
		"2020-06",
		"2020-07",
		"2020-08",
		"2020-09",
		"2020-10",
		"2020-11",
		"2020-12",
		"2021-01",
		"2021-02",
		"2021-03",
		"2021-04",
		"2021-05",
		"2021-06",
		"2021-07",
		"2021-08",
		"2021-09",
		"2021-10",
		"2021-11",
		"2021-12",
		"2022-01",
		"2022-02",
		"2022-03",
		"2022-04",
		"2022-05",
		"2022-06",
		"2022-07",
		"2022-08",
		"2022-09",
		"2022-10",
		"2022-11",
		"2022-12",
		"2023-01",
		"2023-02",
		"2023-03",
		"2023-04",
		"2023-05",
		"2023-06",
		"2023-07",
		"2023-08",
		"2023-09",
		"2023-10",
		"2023-11",
		"2023-12",
	}
}
