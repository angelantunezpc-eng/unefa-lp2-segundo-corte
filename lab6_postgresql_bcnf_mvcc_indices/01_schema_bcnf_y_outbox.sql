BEGIN;

CREATE TABLE IF NOT EXISTS catalogo_planes_bcnf (
    plan_id VARCHAR(32) PRIMARY KEY,
    nombre_plan VARCHAR(100) NOT NULL,
    tarifa_mensual_centavos BIGINT NOT NULL CHECK (tarifa_mensual_centavos >= 0)
);

CREATE TABLE IF NOT EXISTS abonados_bcnf (
    abonado_id VARCHAR(32) PRIMARY KEY,
    msisdn VARCHAR(20) NOT NULL UNIQUE,
    plan_id VARCHAR(32) NOT NULL REFERENCES catalogo_planes_bcnf(plan_id),
    saldo_centavos BIGINT NOT NULL CHECK (saldo_centavos >= 0),
    actualizado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transacciones_bss (
    transaccion_id BIGSERIAL PRIMARY KEY,
    idempotency_key VARCHAR(64) NOT NULL UNIQUE,
    abonado_id VARCHAR(32) NOT NULL REFERENCES abonados_bcnf(abonado_id),
    monto_centavos BIGINT NOT NULL CHECK (monto_centavos > 0),
    metadata_jsonb JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(32) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload_jsonb JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

INSERT INTO catalogo_planes_bcnf (plan_id, nombre_plan, tarifa_mensual_centavos)
VALUES ('PLAN-5G-MAX', 'Plan 5G Ilimitado', 2500)
ON CONFLICT (plan_id) DO NOTHING;

COMMIT;