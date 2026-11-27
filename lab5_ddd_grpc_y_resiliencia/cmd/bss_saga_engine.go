package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrMonedaIncompatible = errors.New("monedas incompatibles entre valores Dinero")
	ErrMontoInvalido      = errors.New("el monto en centavos debe ser mayor que cero")
	ErrSaldoInsuficiente  = errors.New("saldo insuficiente: no se permiten saldos negativos")
	ErrCircuitoAbierto    = errors.New("circuit breaker en estado OPEN: llamada rechazada")
	ErrPasoSagaFallido    = errors.New("fallo en paso de la saga")
)

type Dinero struct {
	montoCentavos int64
	moneda        string
}

func NuevoDinero(centavos int64, moneda string) (Dinero, error) {
	if moneda == "" {
		return Dinero{}, errors.New("la moneda es obligatoria")
	}
	if centavos < 0 {
		return Dinero{}, ErrSaldoInsuficiente
	}
	return Dinero{montoCentavos: centavos, moneda: moneda}, nil
}

func (d Dinero) Centavos() int64 { return d.montoCentavos }
func (d Dinero) Moneda() string  { return d.moneda }

func (d Dinero) Sumar(otro Dinero) (Dinero, error) {
	if d.moneda != otro.moneda {
		return Dinero{}, ErrMonedaIncompatible
	}
	if otro.montoCentavos < 0 {
		return Dinero{}, ErrMontoInvalido
	}
	return Dinero{montoCentavos: d.montoCentavos + otro.montoCentavos, moneda: d.moneda}, nil
}

func (d Dinero) Restar(otro Dinero) (Dinero, error) {
	if d.moneda != otro.moneda {
		return Dinero{}, ErrMonedaIncompatible
	}
	if otro.montoCentavos < 0 {
		return Dinero{}, ErrMontoInvalido
	}
	if d.montoCentavos < otro.montoCentavos {
		return Dinero{}, ErrSaldoInsuficiente
	}
	return Dinero{montoCentavos: d.montoCentavos - otro.montoCentavos, moneda: d.moneda}, nil
}

type CargoAplicadoEvent struct {
	EventID        string
	IdempotencyKey string
	SuscripcionID  string
	CargoCentavos  int64
	SaldoRestante  int64
	Moneda         string
	OcurridoEn     time.Time
}

type Suscripcion5G struct {
	ID      string
	MSISDN  string
	Saldo   Dinero
	Eventos []CargoAplicadoEvent
}

func NuevaSuscripcion5G(id, msisdn string, saldoInicial Dinero) *Suscripcion5G {
	return &Suscripcion5G{
		ID:      id,
		MSISDN:  msisdn,
		Saldo:   saldoInicial,
		Eventos: make([]CargoAplicadoEvent, 0),
	}
}

func (s *Suscripcion5G) DebitarSesion(idempotencyKey string, cargo Dinero, ahora time.Time) (CargoAplicadoEvent, error) {
	if cargo.Centavos() <= 0 {
		return CargoAplicadoEvent{}, ErrMontoInvalido
	}
	nuevoSaldo, err := s.Saldo.Restar(cargo)
	if err != nil {
		return CargoAplicadoEvent{}, err
	}
	s.Saldo = nuevoSaldo
	ev := CargoAplicadoEvent{
		EventID:        fmt.Sprintf("EVT-%d-%s", ahora.UTC().UnixNano(), idempotencyKey),
		IdempotencyKey: idempotencyKey,
		SuscripcionID:  s.ID,
		CargoCentavos:  cargo.Centavos(),
		SaldoRestante:  s.Saldo.Centavos(),
		Moneda:         s.Saldo.Moneda(),
		OcurridoEn:     ahora.UTC(),
	}
	s.Eventos = append(s.Eventos, ev)
	return ev, nil
}

func (s *Suscripcion5G) AcreditarCompensacion(reembolso Dinero) error {
	nuevoSaldo, err := s.Saldo.Sumar(reembolso)
	if err != nil {
		return err
	}
	s.Saldo = nuevoSaldo
	return nil
}

type EstadoCircuito string

const (
	EstadoClosed   EstadoCircuito = "CLOSED"
	EstadoOpen     EstadoCircuito = "OPEN"
	EstadoHalfOpen EstadoCircuito = "HALF-OPEN"
)

type CircuitBreaker struct {
	mu                 sync.Mutex
	estado             EstadoCircuito
	fallosConsecutivos int
	umbralFallos       int
	tiempoEspera       time.Duration
	abiertoEn          time.Time
}

func NuevoCircuitBreaker(umbralFallos int, tiempoEspera time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		estado:       EstadoClosed,
		umbralFallos: umbralFallos,
		tiempoEspera: tiempoEspera,
	}
}

func (cb *CircuitBreaker) Ejecutar(fn func() error) error {
	cb.mu.Lock()
	if cb.estado == EstadoOpen {
		if time.Since(cb.abiertoEn) >= cb.tiempoEspera {
			cb.estado = EstadoHalfOpen
		} else {
			cb.mu.Unlock()
			return ErrCircuitoAbierto
		}
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if err != nil {
		cb.fallosConsecutivos++
		if cb.estado == EstadoHalfOpen || cb.fallosConsecutivos >= cb.umbralFallos {
			cb.estado = EstadoOpen
			cb.abiertoEn = time.Now()
		}
		return err
	}

	cb.fallosConsecutivos = 0
	cb.estado = EstadoClosed
	return nil
}

func (cb *CircuitBreaker) EstadoActual() EstadoCircuito {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.estado
}

func BackoffFullJitter(attempt int) time.Duration {
	const baseMs = 25.0
	const maxMs = 400.0
	if attempt < 0 {
		attempt = 0
	}
	exp := math.Min(maxMs, baseMs*math.Pow(2, float64(attempt)))
	jitterMs := rand.Float64() * exp
	return time.Duration(jitterMs * float64(time.Millisecond))
}

type SagaOrchestrator struct {
	suscripcion       *Suscripcion5G
	cb                *CircuitBreaker
	fallarQoS5G       bool
	fallarFactura     bool
	compensaciones    []string
	clavesCompensadas map[string]bool
}

func NuevoSagaOrchestrator(sub *Suscripcion5G, cb *CircuitBreaker, fallarQoS5G, fallarFactura bool) *SagaOrchestrator {
	return &SagaOrchestrator{
		suscripcion:       sub,
		cb:                cb,
		fallarQoS5G:       fallarQoS5G,
		fallarFactura:     fallarFactura,
		compensaciones:    make([]string, 0),
		clavesCompensadas: make(map[string]bool),
	}
}

func (o *SagaOrchestrator) ejecutarCompensacion(clave string, desc string, fn func() error) {
	if o.clavesCompensadas[clave] {
		return
	}
	if err := fn(); err == nil {
		o.clavesCompensadas[clave] = true
		o.compensaciones = append(o.compensaciones, desc)
	}
}

func (o *SagaOrchestrator) Ejecutar(ctx context.Context, idempotencyKey string, cargo Dinero) error {
	_ = ctx
	var debitado bool

	t1 := func() error {
		_, err := o.suscripcion.DebitarSesion(idempotencyKey, cargo, time.Now())
		if err == nil {
			debitado = true
		}
		return err
	}

	t2 := func() error {
		if o.fallarQoS5G {
			return fmt.Errorf("%w: T2 Aprovisionar QoS 5G", ErrPasoSagaFallido)
		}
		return nil
	}

	t3 := func() error {
		if o.fallarFactura {
			return fmt.Errorf("%w: T3 Emitir Factura", ErrPasoSagaFallido)
		}
		return nil
	}

	if err := o.cb.Ejecutar(t1); err != nil {
		return err
	}

	if err := o.cb.Ejecutar(t2); err != nil {
		if debitado {
			o.ejecutarCompensacion(
				idempotencyKey+":C1",
				"C1: Reembolsar billetera (idempotency_key="+idempotencyKey+")",
				func() error { return o.suscripcion.AcreditarCompensacion(cargo) },
			)
		}
		return err
	}

	if err := o.cb.Ejecutar(t3); err != nil {
		o.ejecutarCompensacion(
			idempotencyKey+":C2",
			"C2: Liberar perfil QoS 5G (idempotency_key="+idempotencyKey+")",
			func() error { return nil },
		)
		if debitado {
			o.ejecutarCompensacion(
				idempotencyKey+":C1",
				"C1: Reembolsar billetera (idempotency_key="+idempotencyKey+")",
				func() error { return o.suscripcion.AcreditarCompensacion(cargo) },
			)
		}
		return err
	}

	return nil
}

func main() {
	saldoInicial, _ := NuevoDinero(50000, "USD")
	cargo, _ := NuevoDinero(12000, "USD")
	sub := NuevaSuscripcion5G("SUB-5G-01", "+584121234567", saldoInicial)
	cb := NuevoCircuitBreaker(2, 200*time.Millisecond)

	for i := 0; i < 3; i++ {
		_ = BackoffFullJitter(i)
	}

	sagaFallida := NuevoSagaOrchestrator(sub, cb, true, false)
	err := sagaFallida.Ejecutar(context.Background(), "IDEMP-SAGA-001", cargo)
	fmt.Printf("Saga con fallo en T2 -> err=%v | saldo=%d | compensaciones=%v | CB=%s\n",
		err, sub.Saldo.Centavos(), sagaFallida.compensaciones, cb.EstadoActual())

	sagaExitosa := NuevoSagaOrchestrator(sub, cb, false, false)
	errOk := sagaExitosa.Ejecutar(context.Background(), "IDEMP-SAGA-002", cargo)
	fmt.Printf("Saga exitosa -> err=%v | saldo=%d | eventos=%d\n",
		errOk, sub.Saldo.Centavos(), len(sub.Eventos))
}
