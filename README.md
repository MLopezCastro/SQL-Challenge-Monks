
---

# SQL Challenge – Media.Monks (BigQuery + Looker Studio)

**Repo:** `SQL-Challenge-Monks`
**Dataset origen:** `mm-tse-latam-interviews.challange_marcelo`
**Tablas:** `ventas`, `productos`, `tdc`
**Período esperado:** **2022-01-01 → 2022-03-31** (Argentina, Brasil, México)

---

## Enfoque

* **Fase 1 – EDA / Data Profiling (esta sección):** observar la calidad de datos, cuantificar problemas y dejar criterios de limpieza **justificados**.
* **Fase 2 – Limpieza (al final):** materializar `VENTAS_LIMPIA` aplicando las reglas acordadas.
* **Fase 3 – Insights (luego):** calcular ingresos en USD y ranking mensual por país (Looker Studio).

> Nota: EDA = *Exploratory Data Analysis*. Acá no “forzamos” datos; **documentamos** lo que hay y definimos **reglas**. Recién después creamos la tabla limpia.

---

## Fase 1 — EDA / Data Profiling (VENTAS)

### 1) Tamaño y fechas

* **Filas totales:** **6,000**
* **Rango observado:** **2022-01-01 → 2022-03-31**

```sql
SELECT COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;

SELECT MIN(creation_date) AS min_fecha, MAX(creation_date) AS max_fecha
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;
```

---

### 2) País (valores crudos)

Se observan variantes de escritura:

| pais | filas |
| ---: | ----: |
|  Arg | 1,925 |
|  Mex | 1,918 |
|  Bra | 1,911 |
|   Br |    89 |
|   Mx |    82 |
|   Ar |    75 |

```sql
SELECT pais, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
GROUP BY pais
ORDER BY filas DESC;
```

**Decisión:** normalizar a `AR/BR/MX` (mapeo por prefijo o lista) y **descartar** valores no mapeables.

---

### 3) `id_venta`

* **Vacío / nulo:** **244**
* **Duplicados (en no-vacíos):** **0**

```sql
-- nulos/vacíos
SELECT COUNT(*) AS id_venta_vacio
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE id_venta IS NULL OR TRIM(id_venta) = '';

-- duplicados (no devolvió filas)
SELECT id_venta, COUNT(*) AS repeticiones
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE id_venta IS NOT NULL AND TRIM(id_venta) <> ''
GROUP BY id_venta
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC
LIMIT 50;
```

**Decisión:** `id_venta` es clave ⇒ **excluir** vacíos/nulos. Si aparecieran duplicados en futuras cargas, conservar **fecha más reciente** y, en empate, **mayor cantidad**.

---

### 4) Cantidades y precios

* **`cantidad ≤ 0` (o nula):** **489** (**8.15%**)
* **`precio_moneda_local ≤ 0` (o nula):** **0**

```sql
SELECT COUNT(*) AS cant_no_positiva
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE cantidad IS NULL OR cantidad <= 0;

SELECT COUNT(*) AS precio_no_positivo
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE precio_moneda_local IS NULL OR precio_moneda_local <= 0;
```

**Decisión:** no invertimos signos (p.ej. `-6 → 6`); el enunciado no modela devoluciones. Tratamos `cantidad ≤ 0` como **errores de carga** ⇒ **excluir** en `VENTAS_LIMPIA`.

---

### 5) Fechas fuera de rango

* **Filas fuera de 2022-01-01 ↔ 2022-03-31:** **0**

```sql
SELECT COUNT(*) AS fechas_fuera_de_rango
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE creation_date < DATE '2022-01-01'
   OR creation_date > DATE '2022-03-31';
```

---

### 6) Integridad entre tablas

**6.A) `ventas` ↔ `productos`**

* **Resultado:** `productos_sin_match = 0` (todo `id_producto` existe en `productos`)

```sql
SELECT COUNT(*) AS productos_sin_match
FROM `mm-tse-latam-interviews.challange_marcelo.ventas` v
LEFT JOIN `mm-tse-latam-interviews.challange_marcelo.productos` p
  ON CAST(v.id_producto AS STRING) = CAST(p.id_producto AS STRING)
WHERE p.id_producto IS NULL;
```

**6.B) Cobertura de TDC por país/fecha**
En `tdc` los países vienen con **variantes** (`Arg/Arg1`, `Bra/Bra2`, `Mex/Mex3`). Hay que **normalizar también TDC** para poder unir por país/fecha.

```sql
-- Conteo de ventas que quedarían sin TDC (tras normalizar ambos lados)
WITH v_norm AS (
  SELECT
    creation_date,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(pais), r'^arg') THEN 'AR'
      WHEN REGEXP_CONTAINS(LOWER(pais), r'^bra') THEN 'BR'
      WHEN REGEXP_CONTAINS(LOWER(pais), r'^mex') THEN 'MX'
      ELSE NULL
    END AS pais_norm
  FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
),
t_norm AS (
  SELECT
    DATE(fecha_tdc) AS fecha,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(pais), r'^arg') THEN 'AR'
      WHEN REGEXP_CONTAINS(LOWER(pais), r'^bra') THEN 'BR'
      WHEN REGEXP_CONTAINS(LOWER(pais), r'^mex') THEN 'MX'
      ELSE NULL
    END AS pais_norm,
    AVG(tdc) AS tdc          -- consolido variantes por día/país
  FROM `mm-tse-latam-interviews.challange_marcelo.tdc`
  GROUP BY fecha, pais_norm
)
SELECT COUNT(*) AS sin_tdc
FROM v_norm v
LEFT JOIN t_norm t
  ON t.pais_norm = v.pais_norm
 AND t.fecha     = v.creation_date
WHERE v.pais_norm IS NOT NULL AND t.tdc IS NULL;
```

> **Resultado a completar:** `sin_tdc = ____`
> **Criterio:** para simplificar, **no exijo TDC en `VENTAS_LIMPIA`**. El TDC se **normaliza y exige** recién al calcular USD (Ejercicio 2); si no hay TDC ese día/país, la venta **no entra** en el cálculo USD.

---

## Reglas de limpieza acordadas (se aplicarán en Fase 2)

1. **País** → normalizar a `AR/BR/MX`; descartar valores no mapeables.
2. **`id_venta`** → obligatorio (excluir `NULL`/vacío).
3. **`creation_date`** → dentro de `2022-01-01`–`2022-03-31`.
4. **`id_producto`** → obligatorio y **debe existir** en `productos`.
5. **`cantidad`** → **> 0** (excluir `≤ 0`).
6. **`precio_moneda_local`** → **> 0**.
7. **Deduplicación** por `id_venta` (defensivo): conservar **fecha más reciente** y, en empate, **mayor cantidad**.
8. **TDC** → **no** se exige en `VENTAS_LIMPIA`; se normaliza y exige al convertir a **USD**.

---

## Fase 2 — Limpieza (crear `VENTAS_LIMPIA`)

> **A ejecutar cuando cierres el EDA.** Implementa todas las reglas (excepto TDC).

```sql
CREATE OR REPLACE TABLE `mm-tse-latam-interviews.challange_marcelo.VENTAS_LIMPIA` AS
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
dedupe AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id_venta
      ORDER BY creation_date DESC, cantidad DESC
    ) AS rn
  FROM calidad
),
con_producto AS (
  SELECT d.*
  FROM dedupe d
  JOIN `mm-tse-latam-interviews.challange_marcelo.productos` p
    ON CAST(p.id_producto AS STRING) = d.id_producto
  WHERE rn = 1
)
SELECT id_venta, creation_date, pais, id_producto, cantidad, precio_moneda_local
FROM con_producto;
```

**Verificación rápida**

```sql
SELECT 'original' AS origen, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
UNION ALL
SELECT 'limpia'   AS origen, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.VENTAS_LIMPIA`;
```

---

## (Adelanto Fase 3) USD y ranking mensual – consultas base

> El TDC se normaliza en una vista auxiliar y se exige al calcular USD.

```sql
-- Vista auxiliar: TDC normalizado por día/país
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_tdc_norm` AS
SELECT
  DATE(fecha_tdc) AS fecha,
  CASE
    WHEN REGEXP_CONTAINS(LOWER(pais), r'^arg') THEN 'AR'
    WHEN REGEXP_CONTAINS(LOWER(pais), r'^bra') THEN 'BR'
    WHEN REGEXP_CONTAINS(LOWER(pais), r'^mex') THEN 'MX'
    ELSE NULL
  END AS pais,
  AVG(tdc) AS tdc
FROM `mm-tse-latam-interviews.challange_marcelo.tdc`
GROUP BY fecha, pais;

-- Ventas en USD (filtra filas sin match de TDC)
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd` AS
SELECT
  v.creation_date,
  FORMAT_DATE('%Y-%m', v.creation_date) AS ym,
  v.pais,
  v.id_producto,
  v.cantidad,
  v.precio_moneda_local,
  (v.cantidad * v.precio_moneda_local) / t.tdc AS importe_usd
FROM `mm-tse-latam-interviews.challange_marcelo.VENTAS_LIMPIA` v
JOIN `mm-tse-latam-interviews.challange_marcelo.v_tdc_norm` t
  ON t.pais = v.pais AND t.fecha = v.creation_date;

-- Ranking mensual por país (top N)
SELECT
  pais,
  ym,
  id_producto,
  SUM(importe_usd) AS usd_total,
  RANK() OVER (PARTITION BY pais, ym ORDER BY SUM(importe_usd) DESC) AS rk
FROM `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd`
GROUP BY pais, ym, id_producto
ORDER BY pais, ym, rk;
```

Con estos resultados podés responder:

1. **Estabilidad** de ventas por producto y país (variación mensual del `usd_total`).
2. **Diferencias entre países** (comparar `usd_total` por producto entre `AR/BR/MX`).

---

## Notas finales

* **No** alteramos datos (p.ej., no cambiamos `-6` a `6`). Preferimos excluir inconsistencias y **documentar**.
* `VENTAS_LIMPIA` es **reproducible** y **trazable** desde este README.
* El TDC se **normaliza** en una vista separada para mantener `VENTAS_LIMPIA` simple y usarla en distintos análisis.

---

**Listo.** Fase 1 (EDA) documentada y criterios cerrados. Cuando quieras, corrés el bloque de **Fase 2** para crear `VENTAS_LIMPIA` y seguimos con el ranking en USD.









