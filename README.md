
---

# SQL Challenge – Media.Monks (BigQuery + Looker Studio)

**Repo:** `SQL-Challenge-Monks`
**Dataset origen:** `mm-tse-latam-interviews.challange_marcelo`
**Tablas:** `ventas`, `productos`, `tdc`
**Período esperado:** **2022-01-01 → 2022-03-31** (AR, BR, MX)

---

## 🧭 Enfoque (por fases)

* **Fase 1 – EDA / Data Profiling:** observar y medir calidad de datos, dejar reglas claras (lo que se hace y por qué).
* **Fase 2 – Limpieza:** crear **`VENTAS_LIMPIA`** con esas reglas (sin inventar datos).
* **Fase 3 – Insights (Ej. 2):** convertir a **USD** y armar el **ranking mensual por país**; responder preguntas de **estabilidad** y **diferencias entre países**.

> **Decisión clave:** **No** convertir cantidades negativas a positivas (p.ej. `-6 → 6`). El enunciado no modela devoluciones; tratamos `cantidad ≤ 0` como **registro inválido** para el objetivo del challenge y lo **excluimos** de la tabla limpia.

---

## 📊 Fase 1 — EDA / Data Profiling (VENTAS)

### 1) Tamaño y rango temporal

| Métrica       |          Valor |
| ------------- | -------------: |
| Filas totales |      **6,000** |
| Fecha mínima  | **2022-01-01** |
| Fecha máxima  | **2022-03-31** |

**Queries**

```sql
SELECT COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;

SELECT MIN(creation_date) AS min_fecha, MAX(creation_date) AS max_fecha
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;
```

---

### 2) País (valores crudos) → normalización

| pais (crudo) | filas |
| -----------: | ----: |
|          Arg | 1,925 |
|          Mex | 1,918 |
|          Bra | 1,911 |
|           Br |    89 |
|           Mx |    82 |
|           Ar |    75 |

**Hallazgo:** hay variantes (`Arg/Ar`, `Bra/Br`, `Mex/Mx`).
**Regla:** normalizar a **`AR`/`BR`/`MX`** y descartar valores no mapeables.

**Query**

```sql
SELECT pais, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
GROUP BY pais
ORDER BY filas DESC;
```

---

### 3) Clave `id_venta`

| Chequeo                   | Resultado |
| ------------------------- | --------: |
| `id_venta` vacío/nulo     |   **244** |
| Duplicados (en no-vacíos) |     **0** |

**Regla:** `id_venta` es **obligatorio** → excluir vacíos/nulos.
Si en futuras cargas hubiera duplicados, conservar **fecha más reciente** y, si empata, **mayor cantidad**.

**Queries**

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

---

### 4) Cantidad y precio

| Chequeo                            |           Resultado |
| ---------------------------------- | ------------------: |
| `cantidad ≤ 0` (o nula)            | **489** (**8.15%**) |
| `precio_moneda_local ≤ 0` (o nula) |               **0** |

**Regla:** no invertimos signos; **excluir** `cantidad ≤ 0`.

**Queries**

```sql
SELECT COUNT(*) AS cant_no_positiva
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE cantidad IS NULL OR cantidad <= 0;

SELECT COUNT(*) AS precio_no_positivo
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE precio_moneda_local IS NULL OR precio_moneda_local <= 0;
```

---

### 5) Fechas fuera de rango

| Chequeo                          | Resultado |
| -------------------------------- | --------: |
| Fuera de 2022-01-01 ↔ 2022-03-31 |     **0** |

```sql
SELECT COUNT(*) AS fechas_fuera_de_rango
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE creation_date < DATE '2022-01-01'
   OR creation_date > DATE '2022-03-31';
```

---

### 6) Integridad entre tablas

**6.A) `ventas` vs `productos`**
**Resultado:** **0** productos huérfanos (`id_producto` siempre existe en `productos`).

```sql
SELECT COUNT(*) AS productos_sin_match
FROM `mm-tse-latam-interviews.challange_marcelo.ventas` v
LEFT JOIN `mm-tse-latam-interviews.challange_marcelo.productos` p
  ON CAST(v.id_producto AS STRING) = CAST(p.id_producto AS STRING)
WHERE p.id_producto IS NULL;
```

**6.B) `tdc` (tipo de cambio)**
`tdc.pais` viene con variantes (`Arg/Arg1`, `Bra/Bra2`, `Mex/Mex3`).
Para **USD** (Ej. 2) normalizaremos `tdc` (por prefijo) y lo exigiremos **al convertir**, no en la tabla limpia.

---

## ✅ Reglas de limpieza (lo que aplica VENTAS\_LIMPIA)

* **País:** normalizar a `AR/BR/MX`; descartar no mapeables.
* **`id_venta`:** **INT64**, obligatorio (sin nulos/vacíos).
* **`creation_date`:** dentro de `2022-01-01`–`2022-03-31`.
* **`id_producto`:** obligatorio (validado en EDA).
* **`cantidad`:** **> 0**.
* **`precio_moneda_local`:** **> 0**.
* **Deduplicación por `id_venta`:** conservar **fecha más reciente** y, si empata, **mayor cantidad**.
* **TDC:** **no** se exige en `VENTAS_LIMPIA`; se usará al convertir a **USD**.

> 💡 **Por qué SIN JOIN acá:** ya verificamos que todos los `id_producto` existen; el JOIN solo complicaba sin aportar valor. Mantengo la creación **simple y reproducible**.

---

## 🧼 Fase 2 — Creación de `VENTAS_LIMPIA` (versión oficial, sin joins)

```sql
CREATE OR REPLACE TABLE `mm-tse-latam-interviews.challange_marcelo.VENTAS_LIMPIA` AS
WITH base AS (
  SELECT
    -- id_venta como INT64; SAFE_CAST evita errores y descarta vacíos
    SAFE_CAST(NULLIF(TRIM(CAST(id_venta AS STRING)), '') AS INT64) AS id_venta,
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
  WHERE id_venta IS NOT NULL
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
)
SELECT
  id_venta, creation_date, pais, id_producto, cantidad, precio_moneda_local
FROM dedupe
WHERE rn = 1;
```

**Verificación (conteo)**

```sql
SELECT 'original' AS origen, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
UNION ALL
SELECT 'limpia'   AS origen, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.VENTAS_LIMPIA`;
```

---

## 💵 Fase 3 — Preparado para Ejercicio 2 (USD + ranking)

> El TDC se **normaliza** y se exige **al calcular USD**, no en la tabla limpia.

**Vista auxiliar – TDC normalizado (por prefijo y día/país):**

```sql
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
```

**Ventas en USD (exige match de TDC):**

```sql
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
```

**Ranking mensual por país (para tablero):**

```sql
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

**Guía rápida para responder las preguntas del Ej. 2**

* **Estabilidad mensual (por país):** calcular `usd_total` por producto y mes; evaluar **coeficiente de variación** `sd/avg` (menor = más estable).
* **Diferencias entre países:** sumar `usd_total` por producto y comparar **gap** `max - min` o **ratio** `max/min` entre `AR/BR/MX`.

---

## ✅ Checklist de entrega

* [x] EDA documentado con métricas y decisiones.
* [x] `VENTAS_LIMPIA` creada con reglas claras (**sin joins**).
* [ ] Vistas para USD y ranking ejecutadas.
* [ ] Respuestas a estabilidad y diferencias entre países.
* [ ] Tablero de Looker Studio conectado a las vistas.

---

Si este te cierra, usalo como **versión definitiva**. De nuevo: perdón por el quilombo anterior.














