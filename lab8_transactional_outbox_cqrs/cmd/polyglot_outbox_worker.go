package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type OutboxEvent struct {
	EventID       int64
	AggregateID   string
	EventType     string
	PayloadJSON   string
	Status        string
	IdempotencyID string
}

type OutboxBroker struct {
	mu          sync.Mutex
	eventos     []*OutboxEvent
	proyectados map[string]bool
}

func NuevoOutboxBroker() *OutboxBroker {
	return &OutboxBroker{
		eventos:     make([]*OutboxEvent, 0),
		proyectados: make(map[string]bool),
	}
}

func (b *OutboxBroker) RegistrarTransaccionACID(id int64, aggID, payload, idempKey string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.eventos = append(b.eventos, &OutboxEvent{
		EventID:       id,
		AggregateID:   aggID,
		EventType:     "CARGO_5G_APLICADO",
		PayloadJSON:   payload,
		Status:        "PENDING",
		IdempotencyID: idempKey,
	})
}

func (b *OutboxBroker) ReclamarPendientesSkipLocked(limite int) []*OutboxEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	lote := make([]*OutboxEvent, 0, limite)
	for _, ev := range b.eventos {
		if ev.Status == "PENDING" {
			ev.Status = "IN_PROGRESS"
			lote = append(lote, ev)
			if len(lote) == limite {
				break
			}
		}
	}
	return lote
}

func (b *OutboxBroker) ProyectarScyllaLocalQuorum(ev *OutboxEvent) error {
	time.Sleep(15 * time.Millisecond)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.proyectados[ev.IdempotencyID] = true
	ev.Status = "PROCESSED"
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = ctx

	broker := NuevoOutboxBroker()
	for i := 1; i <= 6; i++ {
		broker.RegistrarTransaccionACID(
			int64(i),
			fmt.Sprintf("ABN-%d", i),
			fmt.Sprintf(`{"monto_centavos":500,"celda":"CCS-NORTE-01","seq":%d}`, i),
			fmt.Sprintf("IDEMP-OUTBOX-%d", i),
		)
	}

	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			lote := broker.ReclamarPendientesSkipLocked(2)
			for _, ev := range lote {
				if err := broker.ProyectarScyllaLocalQuorum(ev); err == nil {
					fmt.Printf("[Worker %d] Evento %d (%s) -> %s en ScyllaDB (LOCAL_QUORUM)\n",
						workerID, ev.EventID, ev.IdempotencyID, ev.Status)
				}
			}
		}(w)
	}
	wg.Wait()
}
