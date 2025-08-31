# SQL Challenge – Media.Monks (BigQuery + Looker Studio)

**Repo:** SQL-Challenge-Monks
**Dataset origen:** `mm-tse-latam-interviews.challange_marcelo`
**Tablas:** `ventas`, `productos`, `tdc`
**Período esperado:** 2022-01-01 a 2022-03-31 (AR, BR, MX)

---

## 1) Data Profiling – Evidencia (VENTAS)

> Objetivo: inspeccionar calidad de datos, registrar problemas y definir criterios de limpieza.
> Todas las consultas están en BigQuery SQL con el dataset `challange_marcelo`.

### 1.1 Conteo y fechas

**Resultado**

* Filas totales: **6,000**
* Rango observado: **2022-01-01 → 2022-03-31**

**Query**

```sql
SELECT COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;

SELECT MIN(creation_date) AS min_fecha, MAX(creation_date) AS max_fecha
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;
```

---

### 1.2 País (valores crudos y propuesta de normalización)

**Distribución cruda**

| pais | filas |
| ---: | ----: |
|  Arg | 1,925 |
|  Mex | 1,918 |
|  Bra | 1,911 |
|   Br |    89 |
|   Mx |    82 |
|   Ar |    75 |

> Hallazgo: existen variantes (`Arg/Ar`, `Bra/Br`, `Mex/Mx`).
> Decisión: **normalizar** a `AR`, `BR`, `MX`.

**Query (distribución y vista previa de normalización)**

```sql
SELECT pais, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
GROUP BY pais
ORDER BY filas DESC;

SELECT
  pais AS pais_raw,
  CASE
    WHEN LOWER(pais) IN ('ar','arg','argentina') THEN 'AR'
    WHEN LOWER(pais) IN ('br','bra','brasil','brazil') THEN 'BR'
    WHEN LOWER(pais) IN ('mx','mex','méxico','mexico') THEN 'MX'
    ELSE NULL
  END AS pais_norm,
  COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
GROUP BY pais_raw, pais_norm
ORDER BY filas DESC;
```

---

### 1.3 `id_venta` (nulos/vacíos y duplicados)

**Resultados**

* `id_venta` vacío/nulo: **244**
* Duplicados sobre `id_venta` no vacío: **0**

**Query**

```sql
SELECT COUNT(*) AS id_venta_vacio
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE id_venta IS NULL OR TRIM(id_venta) = '';

SELECT id_venta, COUNT(*) AS repeticiones
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE id_venta IS NOT NULL AND TRIM(id_venta) <> ''
GROUP BY id_venta
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC
LIMIT 50;
```

**Criterio**

* `id_venta` es clave obligatoria → **excluir** filas con valor vacío/nulo.
* Si hubiera duplicados: conservar **fecha más reciente** y, en empate, **mayor cantidad** (no aplica aquí).

---

### 1.4 Cantidades y precios no válidos

**Resultados**

* `cantidad` ≤ 0 o nula: **489**
* `precio_moneda_local` ≤ 0 o nula: **0**

**Query**

```sql
SELECT COUNT(*) AS cant_no_positiva
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE cantidad IS NULL OR cantidad <= 0;

SELECT COUNT(*) AS precio_no_positivo
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE precio_moneda_local IS NULL OR precio_moneda_local <= 0;
```

**Criterio**

* Tratar `cantidad` ≤ 0 como errores de carga (no modelamos devoluciones) → **excluir**.

---

### 1.5 Fechas fuera de rango

**Resultado**

* Filas fuera de 2022-01-01 a 2022-03-31: **0**

**Query**

```sql
SELECT COUNT(*) AS fechas_fuera_de_rango
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE creation_date < DATE '2022-01-01'
   OR creation_date > DATE '2022-03-31';
```

---

### 1.6 Integridad (pendiente de ejecutar y documentar)

**A) `ventas` vs `productos`**

```sql
-- Ajustar tipos para comparar correctamente
SELECT COUNT(*) AS productos_sin_match
FROM `mm-tse-latam-interviews.challange_marcelo.ventas` v
LEFT JOIN `mm-tse-latam-interviews.challange_marcelo.productos` p
  ON CAST(v.id_producto AS STRING) = CAST(p.id_producto AS STRING)
WHERE p.id_producto IS NULL;
```

**B) Cobertura de `tdc` por país (normalizado) y fecha**

```sql
WITH v_norm AS (
  SELECT
    creation_date,
    CASE
      WHEN LOWER(pais) IN ('ar','arg','argentina') THEN 'AR'
      WHEN LOWER(pais) IN ('br','bra','brasil','brazil') THEN 'BR'
      WHEN LOWER(pais) IN ('mx','mex','méxico','mexico') THEN 'MX'
      ELSE NULL
    END AS pais_norm
  FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
)
SELECT COUNT(*) AS sin_tdc
FROM v_norm v
LEFT JOIN `mm-tse-latam-interviews.challange_marcelo.tdc` t
  ON t.pais = v.pais_norm AND DATE(t.fecha_tdc) = v.creation_date
WHERE v.pais_norm IS NOT NULL AND t.tdc IS NULL;
```

> Completar resultados aquí cuando se ejecuten y anotar la decisión (excluir o imputar).

---

## 2) Decisiones de limpieza (resumen)

1. **Normalizar `pais`** a `AR/BR/MX`; descartar valores no mapeables.
2. **`id_venta` obligatorio** → excluir **244** filas vacías/nulas.
3. **`cantidad` > 0** → excluir **489** filas con cantidad ≤ 0 o nula.
4. **`precio_moneda_local` > 0** → no se detectaron casos inválidos.
5. **Fechas** dentro del rango → sin exclusiones por este punto.
6. **Integridad** (depende de 1.6):

   * Excluir ventas cuyo `id_producto` no exista en `productos`.
   * Excluir ventas sin `tdc` para su país/fecha (o imputar si se define).

---

## 3) (Opcional) Tabla de auditoría de anomalías

```sql
CREATE OR REPLACE TABLE `mm-tse-latam-interviews.challange_marcelo.VENTAS_ANOMALIAS` AS
WITH base AS (
  SELECT
    v.*,
    CASE
      WHEN LOWER(pais) IN ('ar','arg','argentina') THEN 'AR'
      WHEN LOWER(pais) IN ('br','bra','brasil','brazil') THEN 'BR'
      WHEN LOWER(pais) IN ('mx','mex','méxico','mexico') THEN 'MX'
      ELSE NULL
    END AS pais_norm,
    ARRAY_REMOVE([
      IF(id_venta IS NULL OR TRIM(id_venta) = '', 'id_venta_vacio', NULL),
      IF(creation_date IS NULL, 'fecha_nula', NULL),
      IF(creation_date < DATE '2022-01-01' OR creation_date > DATE '2022-03-31', 'fuera_de_rango', NULL),
      IF(pais IS NULL OR pais_norm IS NULL, 'pais_invalido', NULL),
      IF(cantidad IS NULL OR cantidad <= 0, 'cantidad_no_positiva', NULL),
      IF(precio_moneda_local IS NULL OR precio_moneda_local <= 0, 'precio_no_positivo', NULL)
    ], NULL) AS motivos
  FROM `mm-tse-latam-interviews.challange_marcelo.ventas` v
)
SELECT *
FROM base
WHERE ARRAY_LENGTH(motivos) > 0;

-- Resumen por motivo
SELECT motivo, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.VENTAS_ANOMALIAS`,
UNNEST(motivos) AS motivo
GROUP BY motivo
ORDER BY filas DESC;
```

---

## 4) Implementación de limpieza → crear `VENTAS_LIMPIA`

> Aplica criterios de la sección 2. Este bloque asume que también filtramos según integridad con `productos`.

```sql
-- A) normalizar + filtros de calidad
WITH base AS (
  SELECT
    TRIM(CAST(id_venta AS STRING)) AS id_venta,
    creation_date,
    CASE
      WHEN LOWER(pais) IN ('ar','arg','argentina') THEN 'AR'
      WHEN LOWER(pais) IN ('br','bra','brasil','brazil') THEN 'BR'
      WHEN LOWER(pais) IN ('mx','mex','méxico','mexico') THEN 'MX'
      ELSE NULL
    END AS pais,
    CAST(id_producto AS STRING) AS id_producto,
    CAST(cantidad AS INT64) AS cantidad,
    CAST(precio_moneda_local AS NUMERIC) AS precio_moneda_local
  FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
),
calidad AS (
  SELECT *
  FROM base
  WHERE id_venta IS NOT NULL AND id_venta <> ''
    AND creation_date BETWEEN DATE '2022-01-01' AND DATE '2022-03-31'
    AND pais IN ('AR','BR','MX')
    AND id_producto IS NOT NULL AND id_producto <> ''
    AND cantidad > 0
    AND precio_moneda_local > 0
),

-- B) deduplicación por id_venta (defensivo)
dedupe AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id_venta
      ORDER BY creation_date DESC, cantidad DESC
    ) AS rn
  FROM calidad
),

-- C) asegurar integridad con productos
con_producto AS (
  SELECT d.*
  FROM dedupe d
  JOIN `mm-tse-latam-interviews.challange_marcelo.productos` p
    ON CAST(p.id_producto AS STRING) = d.id_producto
  WHERE rn = 1
)

-- D) materializar tabla final
CREATE OR REPLACE TABLE `mm-tse-latam-interviews.challange_marcelo.VENTAS_LIMPIA` AS
SELECT id_venta, creation_date, pais, id_producto, cantidad, precio_moneda_local
FROM con_producto;
```

**Verificación (trazabilidad)**

```sql
SELECT 'original' AS origen, COUNT(*) FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
UNION ALL
SELECT 'anomalias', COUNT(*) FROM `mm-tse-latam-interviews.challange_marcelo.VENTAS_ANOMALIAS`
UNION ALL
SELECT 'limpia', COUNT(*) FROM `mm-tse-latam-interviews.challange_marcelo.VENTAS_LIMPIA`;
```

---

## 5) Próximos pasos

* Completar **1.6** con resultados e interpretación.
* Crear vista `v_ventas_usd` (join a `tdc`) y luego el **ranking mensual por país** para el Ejercicio 2.
* Construir dashboard en Looker Studio con las vistas resultantes.


