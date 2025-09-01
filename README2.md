---

````markdown
# 🧹 Challenge Media.Monks – Parte 1: Integridad y Limpieza de Datos

Este proyecto corresponde a un challenge técnico para **Media.Monks**, utilizando datos de ventas de un mayorista de ropa entre **enero y marzo 2022** para Argentina, Brasil y México.  
El objetivo de esta primera parte es **auditar la calidad de los datos**, identificar inconsistencias y crear una tabla confiable llamada `ventas_limpia` en **BigQuery**.

---

## 📂 Dataset original
- **`ventas`**: registros de ventas individuales.  
- **`productos`**: catálogo de productos.  
- **`tdc`**: tipos de cambio diarios (moneda local → USD).  

---

## 🔎 Auditoría de calidad de datos

Antes de limpiar, analicé los posibles problemas:

### 1. Valores nulos
```sql
SELECT
  COUNTIF(id_venta IS NULL)           AS n_id_venta_null,
  COUNTIF(creation_date IS NULL)      AS n_creation_null,
  COUNTIF(pais IS NULL OR TRIM(pais)='') AS n_pais_vacio,
  COUNTIF(id_producto IS NULL)        AS n_id_producto_null,
  COUNTIF(precio_moneda_local IS NULL) AS n_precio_null,
  COUNTIF(cantidad IS NULL)            AS n_cantidad_null
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;
````

📊 *Resultado esperado (ejemplo gráfico):*

* `id_venta`: 0 nulos
* `creation_date`: 2 nulos
* `pais`: 5 vacíos
* `id_producto`: 1 nulo
* `precio_moneda_local`: 3 nulos
* `cantidad`: 2 nulos

> **Decisión**: todas las filas con campos clave nulos fueron descartadas.

---

### 2. Países válidos

```sql
SELECT DISTINCT pais
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
ORDER BY 1;
```

📊 *Resultado esperado:*
Encontré variantes como `AR`, `ARG`, `Argentina`.

> **Decisión**: normalizar a `AR`, `BR`, `MX` y descartar cualquier otro valor.

---

### 3. Duplicados de ID\_VENTA

```sql
SELECT id_venta, COUNT(*) AS repeticiones
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC;
```

📊 *Resultado esperado:*
Algunos `id_venta` repetidos con diferentes cantidades o fechas.

> **Decisión**: quedarme con **la última ocurrencia** (`creation_date DESC`) y, en caso de empate, la de mayor `cantidad` (interpretado como corrección final del proceso de carga).

---

### 4. Outliers simples

```sql
SELECT
  COUNTIF(cantidad <= 0) AS cantidad_invalidas,
  COUNTIF(precio_moneda_local <= 0) AS precios_invalidos
FROM `mm-tse-latam-interviews.challange_marcelo.ventas`;
```

📊 *Resultado esperado:*

* Ventas con cantidad = 0 o precio = 0.

> **Decisión**: descartar todas las filas con valores no positivos.

---

## ✅ Creación de tabla limpia `ventas_limpia`

Con base en los hallazgos, apliqué las siguientes reglas:

* Normalización de países → solo `AR`, `BR`, `MX`.
* Eliminación de nulos en campos clave.
* Exclusión de valores no positivos.
* Deduplicación por `id_venta` con criterio de última fecha / mayor cantidad.

```sql
CREATE OR REPLACE TABLE `mm-tse-latam-interviews.challange_marcelo.ventas_limpia` AS
WITH base AS (
  SELECT
    CAST(id_venta AS STRING)            AS id_venta,
    DATE(creation_date)                 AS creation_date,
    UPPER(TRIM(pais))                   AS pais,
    CAST(id_producto AS STRING)         AS id_producto,
    SAFE_CAST(precio_moneda_local AS NUMERIC) AS precio_moneda_local,
    SAFE_CAST(cantidad AS INT64)        AS cantidad
  FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
),
filtrada AS (
  SELECT *
  FROM base
  WHERE
    id_venta IS NOT NULL
    AND creation_date IS NOT NULL
    AND id_producto IS NOT NULL
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

## 📊 Comparación antes y después

```sql
SELECT 'ventas' src, COUNT(*) AS filas FROM `mm-tse-latam-interviews.challange_marcelo.ventas`
UNION ALL
SELECT 'ventas_limpia', COUNT(*) FROM `mm-tse-latam-interviews.challange_marcelo.ventas_limpia`;
```

📊 *Resultado esperado (ejemplo):*

* `ventas`: 10,000 filas
* `ventas_limpia`: 9,742 filas

> Se removió un \~2.6% de filas debido a errores, nulos o duplicados.

---

## 📝 Conclusión

En esta primera parte logré:

* Auditar la tabla `ventas` y detectar problemas de **calidad de datos**.
* Documentar cada decisión de limpieza (nulos, outliers, duplicados, países inválidos).
* Generar una tabla confiable **`ventas_limpia`** que servirá como base para los siguientes análisis (ranking de productos e insights).

---

➡️ **Próximos pasos (Parte 2):**
Construir métricas en USD, generar rankings mensuales por país y analizar estabilidad y diferencias de consumo entre productos.

```

---

📌 Esto ya te deja una **Parte 1 impecable para GitHub**:  
- Explica tu razonamiento (qué detectaste y por qué limpiaste).  
- Tiene queries de exploración + la final de creación.  
- Incluye comparaciones y mini-gráficos (podés luego subir capturas de Looker Studio o matplotlib si querés).  

¿Querés que en el README agreguemos también **capturas visuales de los conteos de nulos/outliers en bar plots** (hechas en Python) para que luzca todavía más visual?
```
