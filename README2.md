Perfecto 💪, acá tenés **TODO el README de la Parte 1 en un solo bloque continuo de texto Markdown**. Lo copiás tal cual en tu `README.md` y se va a ver bien en GitHub (con colores en SQL, tablas, títulos, etc.), sin barras de scroll extrañas en el archivo final.

---

# SQL Challenge – Media.Monks (BigQuery + Looker Studio)

**Parte 1 – Integridad y Limpieza de Datos**

**Repo:** `SQL-Challenge-Monks`
**Proyecto/Dataset:** `mm-tse-latam-interviews.challange_marcelo`
**Tablas origen:** `ventas`, `productos`, `tdc`
**Período esperado:** **2022-01-01 → 2022-03-31** (AR, BR, MX)

---

## 🧭 Enfoque por fases

1. **EDA / Data Profiling:** medir calidad y definir reglas (explico qué y por qué).
2. **Limpieza:** crear **`ventas_limpia`** con reglas reproducibles (sin inventar datos).
3. (Próximo) **Insights:** USD, ranking mensual, estabilidad y diferencias por país.

> **Decisión clave:** no “arreglo” datos inválidos (p. ej., no convierto negativos a positivos). Si `cantidad ≤ 0`, la fila **no es confiable para ingresos** → se excluye.

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

| pais (crudo) | filas |
| -----------: | ----: |
|          Arg | 1,925 |
|          Mex | 1,918 |
|          Bra | 1,911 |
|           Br |    89 |
|           Mx |    82 |
|           Ar |    75 |

**Hallazgo:** hay variantes de mayúsculas/minúsculas/abreviaturas (`Arg/Ar`, `Bra/Br`, `Mex/Mx`).
**Regla:** normalizar a **`AR` / `BR` / `MX`** y descartar lo no mapeable.

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

## ✅ Fase 2 — Creación de `ventas_limpia` (oficial)

**Criterios aplicados**

* Normalizar `pais` → `AR` / `BR` / `MX`.
* Excluir nulos/vacíos en campos clave: `id_venta`, `creation_date`, `id_producto`.
* Excluir `cantidad ≤ 0` y `precio_moneda_local ≤ 0`.
* Deduplicar por `id_venta`: **última** por fecha; si empata, **mayor `cantidad`**.
* *Sin joins en esta fase* (los cruces con `tdc`/`productos` se usarán en la Parte 2).

```sql
CREATE OR REPLACE TABLE `mm-tse-latam-interviews.challange_marcelo.ventas_limpia` AS
WITH base AS (
  SELECT
    CAST(id_venta AS STRING)            AS id_venta,
    DATE(creation_date)                 AS creation_date,
    -- Normalización explícita de país (mapea variantes observadas)
    CASE UPPER(TRIM(pais))
      WHEN 'ARG' THEN 'AR'
      WHEN 'AR'  THEN 'AR'
      WHEN 'BRA' THEN 'BR'
      WHEN 'BR'  THEN 'BR'
      WHEN 'MEX' THEN 'MX'
      WHEN 'MX'  THEN 'MX'
      ELSE NULL
    END                                 AS pais,
    CAST(id_producto AS STRING)         AS id_producto,
    SAFE_CAST(precio_moneda_local AS NUMERIC) AS precio_moneda_local,
    SAFE_CAST(cantidad AS INT64)        AS cantidad
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
    AND pais IN ('AR','BR','MX')
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

> Completar con el resultado al ejecutar en tu dataset (deja evidencia del % filtrado).

---

## ✅ Estado de entrega – Parte 1

* ✔️ EDA documentado con métricas clave.
* ✔️ Reglas de limpieza justificadas.
* ✔️ Tabla **`ventas_limpia`** creada en BigQuery (reproducible).

**Próximo (Parte 2):** métrica en USD (vista), ranking mensual por país, estabilidad por CV y diferencias entre países.
**(Parte 3):** tablero en Looker Studio.

---

---

Así queda limpio, **un solo bloque**, sin cortes.
¿Querés que ahora preparemos de la misma forma la **Parte 2** para que tu README ya tenga todo el challenge completo?

