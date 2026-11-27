# Reporte EXPLAIN (ANALYZE, BUFFERS) — BCNF, MVCC e Índices

## 1. Auditoría MVCC (`ctid`, `xmin`, `xmax`)
- Al actualizar `abonados_bcnf`, PostgreSQL no sobrescribe la fila en sitio: genera una nueva versión física con un nuevo `ctid` y `xmin`, marcando `xmax` en la versión anterior.

## 2. Comparativa `GIN (jsonb_path_ops)` vs `Seq Scan`
- **Antes del índice:** `Seq Scan` sobre `transacciones_bss`, recorriendo todas las páginas de datos y leyendo más buffers.
- **Después del índice (`idx_tx_metadata_gin` + `idx_tx_created_at_brin`):** `Bitmap Index Scan` / `Bitmap Heap Scan`, reduciendo el costo y los buffers leídos en filtros JSONB y rangos temporales (`created_at`).