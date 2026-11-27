INSERT INTO abonados_bcnf (abonado_id, msisdn, plan_id, saldo_centavos)
SELECT
    'ABN-' || g::TEXT,
    '+58412' || LPAD(g::TEXT, 7, '0'),
    'PLAN-5G-MAX',
    10000
FROM generate_series(1, 50000) AS g
ON CONFLICT (abonado_id) DO NOTHING;

UPDATE abonados_bcnf
SET saldo_centavos = saldo_centavos - 500,
    actualizado_en = NOW()
WHERE abonado_id = 'ABN-1';

SELECT ctid, xmin, xmax, abonado_id, saldo_centavos
FROM abonados_bcnf
WHERE abonado_id = 'ABN-1';

INSERT INTO transacciones_bss (idempotency_key, abonado_id, monto_centavos, metadata_jsonb, created_at)
SELECT
    'IDEMP-SEED-' || g::TEXT,
    'ABN-' || (((g - 1) % 50000) + 1)::TEXT,
    1000,
    jsonb_build_object(
        'antena_5g', CASE WHEN g % 25 = 0 THEN 'CCS-NORTE-01' ELSE 'CCS-SUR-02' END,
        'roaming', (g % 25 = 0)
    ),
    NOW() - ((g % 120) || ' hours')::INTERVAL
FROM generate_series(1, 50000) AS g
ON CONFLICT (idempotency_key) DO NOTHING;

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM transacciones_bss
WHERE metadata_jsonb @> '{"antena_5g": "CCS-NORTE-01", "roaming": true}';

CREATE INDEX IF NOT EXISTS idx_tx_metadata_gin
ON transacciones_bss USING GIN (metadata_jsonb jsonb_path_ops);

CREATE INDEX IF NOT EXISTS idx_tx_created_at_brin
ON transacciones_bss USING BRIN (created_at);

ANALYZE transacciones_bss;

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM transacciones_bss
WHERE metadata_jsonb @> '{"antena_5g": "CCS-NORTE-01", "roaming": true}';