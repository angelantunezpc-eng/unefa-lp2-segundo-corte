package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type CDRRecord struct {
	MSISDN          string
	BucketFecha     string
	TimestampEvento time.Time
	IDCDR           string
	Celda5G         string
	BytesConsumidos int64
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func simularEscrituraCDR(rec CDRRecord) time.Duration {
	inicio := time.Now()
	_ = fmt.Sprintf("INSERT INTO unefa_telco_5g.cdr_por_abonado_dia (%s,%s,%s)", rec.MSISDN, rec.BucketFecha, rec.IDCDR)
	time.Sleep(time.Duration(200+rand.Intn(800)) * time.Microsecond)
	return time.Since(inicio)
}

func main() {
	const workers = 16
	const opsPorWorker = 250
	totalOps := workers * opsPorWorker

	latencias := make([]time.Duration, 0, totalOps)
	var mu sync.Mutex
	var wg sync.WaitGroup

	inicioGlobal := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			locales := make([]time.Duration, 0, opsPorWorker)
			for i := 0; i < opsPorWorker; i++ {
				rec := CDRRecord{
					MSISDN:          fmt.Sprintf("+58412%07d", (workerID*opsPorWorker+i)%5000+1),
					BucketFecha:     "2026-11-27",
					TimestampEvento: time.Now().UTC(),
					IDCDR:           fmt.Sprintf("cdr-%d-%d", workerID, i),
					Celda5G:         "CCS-NORTE-01",
					BytesConsumidos: 1048576,
				}
				locales = append(locales, simularEscrituraCDR(rec))
			}
			mu.Lock()
			latencias = append(latencias, locales...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	duracionTotal := time.Since(inicioGlobal)
	sort.Slice(latencias, func(i, j int) bool { return latencias[i] < latencias[j] })

	throughput := float64(len(latencias)) / duracionTotal.Seconds()
	fmt.Printf("Operaciones: %d | Workers: %d | Throughput: %.2f ops/s\n", len(latencias), workers, throughput)
	fmt.Printf("Latencias -> P50: %v | P95: %v | P99: %v\n",
		percentile(latencias, 0.50),
		percentile(latencias, 0.95),
		percentile(latencias, 0.99),
	)
}
