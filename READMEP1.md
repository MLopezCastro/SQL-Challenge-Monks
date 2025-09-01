
---

# SQL Challenge – Media.Monks (BigQuery + Looker Studio)

**Parte 1 – Integridad y Limpieza de Datos (versión final)**

**Repo:** `SQL-Challenge-Monks`
**Proyecto/Dataset:** `mm-tse-latam-interviews.challange_marcelo`
**Tablas origen:** `ventas`, `productos`, `tdc`
**Período esperado:** **2022-01-01 → 2022-03-31** (AR, BR, MX)

---

## 🧭 Enfoque por fases

1. **EDA / Data Profiling:** medir calidad y definir reglas (qué y por qué).
2. **Limpieza:** crear **`ventas_limpia`** con reglas reproducibles (sin inventar datos).
3. (Próximo) **Insights:** USD, ranking mensual, estabilidad y diferencias por país.

> **Decisión clave:** No “arreglo” datos inválidos (p. ej., no convierto negativos a positivos). Si `cantidad ≤ 0`, la fila **no es confiable para ingresos** → se excluye.

---

## 📊 Fase 1 — EDA / Data Profiling sobre `ventas`

### 1) Tamaño y rango temporal

| Métrica       | Valor observado |
| ------------- | --------------: |
| Filas totales |       **6,000** |
| Fecha mínima  |  **2022-01-01** |
| Fecha máxima  |  **2022-03-31** |

**Queries**

```sql
SELECT COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;

SELECT MIN(creation_date) AS min_fecha, MAX(creation_date) AS max_fecha
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;
```

---

### 2) País (valores crudos) → normalización

En los datos crudos se observan variantes inconsistentes por mayúsculas/minúsculas/abreviaturas: `Ar/Arg`, `Br/Bra`, `Mex/Mx`.
**Regla:** normalizar a **`AR` / `BR` / `MX`** con un mapeo robusto y descartar lo no mapeable.

**Query (observación)**

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
Si en futuras cargas aparecen duplicados, conservar **fecha más reciente** y, si empata, **mayor `cantidad`**.

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

### 4) Cantidad y precio (valores no válidos)

| Chequeo                            | Resultado |
| ---------------------------------- | --------: |
| `cantidad ≤ 0` (o nula)            |   **489** |
| `precio_moneda_local ≤ 0` (o nula) |     **0** |

**Regla:** excluir filas con `cantidad ≤ 0` o `precio_moneda_local ≤ 0`.

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

### 5) Fechas fuera de rango (enero–marzo 2022)

| Chequeo                          | Resultado |
| -------------------------------- | --------: |
| Fuera de 2022-01-01 ↔ 2022-03-31 |     **0** |

**Query**

```sql
SELECT COUNT(*) AS fechas_fuera_de_rango
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
WHERE creation_date < DATE '2022-01-01'
   OR creation_date > DATE '2022-03-31';
```

---

## ✅ Fase 2 — Creación de `ventas_limpia` (oficial, mapeo robusto de país)

**Criterios aplicados**

* Normalizar `pais` con mapeo robusto → `AR` / `BR` / `MX`.
* Excluir nulos/vacíos en campos clave: `id_venta`, `creation_date`, `id_producto`.
* Excluir `cantidad ≤ 0` y `precio_moneda_local ≤ 0`.
* Deduplicar por `id_venta`: **última** por fecha; si empata, **mayor `cantidad`**.
* *Sin joins en esta fase* (los cruces con `tdc`/`productos` se usarán en la Parte 2).

```sql
CREATE OR REPLACE TABLE `mm-tse-latam-interviews.challange_marcelo.ventas_limpia` AS
WITH base AS (
  SELECT
    CAST(id_venta AS STRING) AS id_venta,
    DATE(creation_date)      AS creation_date,
    -- Mapeo robusto de país (cubre Ar/Arg, Br/Bra, Mex/Mx y espacios)
    CASE LOWER(TRIM(pais))
      WHEN 'ar'  THEN 'AR'  WHEN 'arg' THEN 'AR'
      WHEN 'br'  THEN 'BR'  WHEN 'bra' THEN 'BR'
      WHEN 'mx'  THEN 'MX'  WHEN 'mex' THEN 'MX'
      ELSE NULL
    END                      AS pais,
    CAST(id_producto AS STRING)               AS id_producto,
    SAFE_CAST(precio_moneda_local AS NUMERIC) AS precio_moneda_local,
    SAFE_CAST(cantidad AS INT64)              AS cantidad
  FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
),
filtrada AS (
  SELECT *
  FROM base
  WHERE
    id_venta IS NOT NULL AND TRIM(id_venta) <> ''
    AND creation_date IS NOT NULL
    AND id_producto IS NOT NULL AND TRIM(id_producto) <> ''
    AND precio_moneda_local IS NOT NULL AND precio_moneda_local > 0
    AND cantidad IS NOT NULL AND cantidad > 0
    AND pais IS NOT NULL  -- ya normalizado a AR/BR/MX
),
dedupe AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT f.*,
           ROW_NUMBER() OVER (
             PARTITION BY id_venta
             ORDER BY creation_date DESC, cantidad DESC
           ) AS rn
    FROM filtrada f
  )
  WHERE rn = 1
)
SELECT * FROM dedupe;
```

---

## 📈 Control de impacto (antes vs. después)

**Query**

```sql
SELECT 'ventas' AS tabla, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
UNION ALL
SELECT 'ventas_limpia' AS tabla, COUNT(*) AS filas
FROM `mm-tse-latam-interviews.challange_marcelo.ventas_limpia`;
```

**Resultado:**

| tabla          | filas |
| -------------- | ----: |
| ventas         |  6000 |
| ventas\_limpia |  5287 |

**Efecto de la limpieza:** se removieron **713 registros (\~11.9%)** por nulos, valores no positivos y normalización de país. La reducción es consistente con las reglas definidas y deja una base confiable para los análisis en USD de la Parte 2.

---

## ✅ Estado de entrega – Parte 1 (final)

* ✔️ EDA documentado con métricas clave.
* ✔️ Reglas de limpieza justificadas.
* ✔️ Tabla **`ventas_limpia`** creada y verificada (**5287 filas**).

**Próximo (Parte 2):** vista en USD (`v_ventas_usd`), ranking mensual por país, estabilidad (CV) y diferencias entre países.
**(Parte 3):** tablero en Looker Studio.

---

