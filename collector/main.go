package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// Trade — структура одной сделки с Binance
type Trade struct {
	Symbol      string `json:"symbol"`
	TradeID     int64  `json:"trade_id"`
	Price       string `json:"price"`
	Quantity    string `json:"quantity"`
	IsBuyer     bool   `json:"is_buyer_maker"`
	Timestamp   int64  `json:"timestamp"`
	CollectedAt string `json:"collected_at"`
}

// binanceTrade — raw-структура из WebSocket
type binanceTrade struct {
	EventType json.RawMessage `json:"e"`
	Symbol    string          `json:"s"`
	TradeID   int64           `json:"t"`
	Price     string          `json:"p"`
	Quantity  string          `json:"q"`
	IsBuyer   bool            `json:"m"`
	Time      int64           `json:"T"`
}

const (
	outputFile = "../data/trades.jsonl"
	batchSize  = 50              // записать при накоплении N записей
	batchTimeout = 5 * time.Second // или каждые T секунд
)

func collectTrades(symbol string, out chan<- Trade, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@trade", symbol)
	log.Printf("[%s] Connecting...", symbol)

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Printf("[%s] Connection error: %v", symbol, err)
		return
	}
	defer func() {
		conn.Close()
		log.Printf("[%s] Connection closed", symbol)
	}()

	log.Printf("[%s] Connected", symbol)

	// Читаем сообщения, пока не придёт сигнал stop
	msgCh := make(chan []byte, 10)
	errCh := make(chan error, 1)

	// Отдельная горутина для чтения из WebSocket
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- msg
		}
	}()

	for {
		select {
		case <-stop:
			log.Printf("[%s] Stop signal received", symbol)
			return
		case err := <-errCh:
			log.Printf("[%s] Read error: %v", symbol, err)
			return
		case msg := <-msgCh:
			var raw binanceTrade
			if err := json.Unmarshal(msg, &raw); err != nil {
				continue
			}
			if raw.Symbol == "" {
				continue
			}
			out <- Trade{
				Symbol:      raw.Symbol,
				TradeID:     raw.TradeID,
				Price:       raw.Price,
				Quantity:    raw.Quantity,
				IsBuyer:     raw.IsBuyer,
				Timestamp:   raw.Time,
				CollectedAt: time.Now().UTC().Format(time.RFC3339),
			}
		}
	}
}

// flushBatch записывает пачку в файл одной операцией
func flushBatch(f *os.File, batch []Trade) {
	if len(batch) == 0 {
		return
	}
	for _, t := range batch {
		line, _ := json.Marshal(t)
		f.Write(line)
		f.WriteString("\n")
	}
	log.Printf("Flushed batch of %d trades", len(batch))
}

func batchWriter(in <-chan Trade, done chan<- struct{}) {
	f, err := os.OpenFile(outputFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Cannot open output file: %v", err)
	}
	defer f.Close()

	batch := make([]Trade, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	total := 0

	for {
		select {
		case trade, ok := <-in:
			if !ok {
				// Канал закрыт — сбрасываем остаток буфера
				flushBatch(f, batch)
				total += len(batch)
				log.Printf("Total written: %d trades", total)
				done <- struct{}{}
				return
			}
			batch = append(batch, trade)
			if len(batch) >= batchSize {
				flushBatch(f, batch)
				total += len(batch)
				batch = batch[:0] // очищаем без аллокации
			}

		case <-ticker.C:
			// Таймаут — сбрасываем что накопилось
			if len(batch) > 0 {
				flushBatch(f, batch)
				total += len(batch)
				batch = batch[:0]
			}
		}
	}
}

func main() {
	symbols := []string{"btcusdt", "ethusdt", "bnbusdt"}
	os.MkdirAll("../data", 0755)

	// Канал с буфером на 200 записей
	trades := make(chan Trade, 200)
	done := make(chan struct{})
	stop := make(chan struct{})

	// Graceful shutdown — ловим SIGINT и SIGTERM
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Printf("Received signal: %v — shutting down gracefully...", sig)
		close(stop) // сигнал всем горутинам-сборщикам
	}()

	// Запускаем batch writer
	go batchWriter(trades, done)

	// Запускаем горутину для каждой пары
	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go collectTrades(sym, trades, stop, &wg)
	}

	// Ждём завершения всех сборщиков
	wg.Wait()
	close(trades) // сигнал writer'у что данных больше не будет
	<-done        // ждём пока writer сбросит буфер

	log.Println("Shutdown complete")
}