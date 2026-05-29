package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Trade — структура одной сделки с Binance
type Trade struct {
	Symbol    string  `json:"symbol"`
	TradeID   int64   `json:"trade_id"`
	Price     string  `json:"price"`
	Quantity  string  `json:"quantity"`
	IsBuyer   bool    `json:"is_buyer_maker"`
	Timestamp int64   `json:"timestamp"`
	CollectedAt string `json:"collected_at"`
}

// binanceTrade — raw-структура из WebSocket (поля Binance)
type binanceTrade struct {
	EventType json.RawMessage `json:"e"`
	Symbol    string          `json:"s"`
	TradeID   int64           `json:"t"`
	Price     string          `json:"p"`
	Quantity  string          `json:"q"`
	IsBuyer   bool            `json:"m"`
	Time      int64           `json:"T"`
}

const outputFile = "../data/trades.jsonl"

func collectTrades(symbol string, out chan<- Trade, wg *sync.WaitGroup) {
	defer wg.Done()

	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@trade", symbol)
	log.Printf("[%s] Connecting to %s", symbol, url)

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Printf("[%s] Connection error: %v", symbol, err)
		return
	}
	defer conn.Close()
	log.Printf("[%s] Connected", symbol)

	// Собираем 30 секунд
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[%s] Read stopped: %v", symbol, err)
			return
		}

		var raw binanceTrade
		if err := json.Unmarshal(msg, &raw); err != nil {
			log.Printf("[%s] Parse error: %v", symbol, err)
			continue
		}

		trade := Trade{
			Symbol:      raw.Symbol,
			TradeID:     raw.TradeID,
			Price:       raw.Price,
			Quantity:    raw.Quantity,
			IsBuyer:     raw.IsBuyer,
			Timestamp:   raw.Time,
			CollectedAt: time.Now().UTC().Format(time.RFC3339),
		}

		out <- trade
	}
}

func writer(out <-chan Trade, done chan<- struct{}) {
	f, err := os.OpenFile(outputFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Cannot open output file: %v", err)
	}
	defer f.Close()

	count := 0
	for trade := range out {
		line, err := json.Marshal(trade)
		if err != nil {
			log.Printf("Marshal error: %v", err)
			continue
		}
		f.Write(line)
		f.WriteString("\n")
		count++
		if count%50 == 0 {
			log.Printf("Written %d trades", count)
		}
	}

	log.Printf("Total written: %d trades", count)
	done <- struct{}{}
}

func main() {
	symbols := []string{"btcusdt", "ethusdt", "bnbusdt"}

	// Создаём папку data если нет
	os.MkdirAll("../data", 0755)

	trades := make(chan Trade, 100)
	done := make(chan struct{})

	// Запускаем writer в отдельной горутине
	go writer(trades, done)

	// Запускаем горутину для каждой пары
	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go collectTrades(sym, trades, &wg)
	}

	// Ждём завершения всех сборщиков, затем закрываем канал
	wg.Wait()
	close(trades)
	<-done

	log.Println("Collection complete")
}