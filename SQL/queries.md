
---

### 📂 SQL

1. **01\_limpieza\_datos.sql**

```sql
-- Creación de tabla limpia de ventas
CREATE OR REPLACE TABLE `mm-tse-latam-interviews.challange_marcelo.ventas_limpia` AS
WITH base AS (
  SELECT
    CAST(id_venta AS STRING) AS id_venta,
    DATE(creation_date)      AS creation_date,
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
  WHERE id_venta IS NOT NULL AND TRIM(id_venta) <> ''
    AND creation_date IS NOT NULL
    AND id_producto IS NOT NULL AND TRIM(id_producto) <> ''
    AND precio_moneda_local IS NOT NULL AND precio_moneda_local > 0
    AND cantidad IS NOT NULL AND cantidad > 0
    AND pais IS NOT NULL
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

2. **02\_tdc\_norm.sql**

```sql
-- Normalización de TDC
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.tdc_norm` AS
SELECT
  fecha_tdc,
  CASE LOWER(TRIM(pais))
    WHEN 'arg'  THEN 'AR'
    WHEN 'arg1' THEN 'AR'
    WHEN 'bra'  THEN 'BR'
    WHEN 'bra2' THEN 'BR'
    WHEN 'mex'  THEN 'MX'
    WHEN 'mex3' THEN 'MX'
    ELSE NULL
  END AS pais,
  tdc
FROM `mm-tse-latam-interviews.challange_marcelo.tdc`;
```

---

3. **03\_v\_ventas\_usd.sql**

```sql
-- Vista de ventas en USD
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd` AS
SELECT
  v.id_venta,
  v.creation_date,
  v.pais,
  v.id_producto,
  p.NOMBRE    AS nombre,
  p.CATEGORIA AS categoria,
  v.cantidad,
  v.precio_moneda_local,
  t.tdc,
  SAFE_DIVIDE(v.precio_moneda_local, t.tdc) AS precio_usd,
  v.cantidad * SAFE_DIVIDE(v.precio_moneda_local, t.tdc) AS ingreso_usd
FROM `mm-tse-latam-interviews.challange_marcelo.ventas_limpia` v
JOIN `mm-tse-latam-interviews.challange_marcelo.tdc_norm` t
  ON t.pais = v.pais
 AND t.fecha_tdc = v.creation_date
JOIN `mm-tse-latam-interviews.challange_marcelo.productos` p
  ON CAST(p.ID_PRODUCTO AS STRING) = v.id_producto;
```

---

4. **04\_v\_mensual\_producto\_pais.sql**

```sql
-- Agregación mensual por producto y país
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_mensual_producto_pais` AS
SELECT
  DATE_TRUNC(creation_date, MONTH) AS mes,
  pais,
  id_producto,
  ANY_VALUE(nombre)    AS nombre,
  ANY_VALUE(categoria) AS categoria,
  SUM(cantidad)        AS unidades,
  SUM(ingreso_usd)     AS ingreso_usd
FROM `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd`
GROUP BY 1,2,3;
```

---

5. **05\_v\_ranking\_mensual.sql**

```sql
-- Ranking mensual por país
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_ranking_mensual` AS
SELECT
  mes,
  pais,
  id_producto,
  nombre,
  categoria,
  ingreso_usd,
  DENSE_RANK() OVER (PARTITION BY pais, mes ORDER BY ingreso_usd DESC) AS rk
FROM `mm-tse-latam-interviews.challange_marcelo.v_mensual_producto_pais`;
```

---

6. **06\_query\_estabilidad.sql**

```sql
-- Producto más estable por país (menor coeficiente de variación)
WITH stats AS (
  SELECT
    pais,
    id_producto,
    nombre,
    AVG(ingreso_usd) AS promedio,
    STDDEV(ingreso_usd) AS desv,
    SAFE_DIVIDE(STDDEV(ingreso_usd), AVG(ingreso_usd)) AS cv
  FROM `mm-tse-latam-interviews.challange_marcelo.v_mensual_producto_pais`
  GROUP BY pais, id_producto, nombre
)
SELECT pais, id_producto, nombre,
       ROUND(promedio,2) AS promedio_usd,
       ROUND(desv,2)     AS desv_usd,
       ROUND(cv,3)       AS cv
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY pais ORDER BY cv ASC) AS rn
  FROM stats
)
WHERE rn = 1
ORDER BY pais;
```

---

7. **07\_query\_gap.sql**

```sql
-- Diferencias de consumo entre países (top productos con mayor gap)
WITH resumen AS (
  SELECT
    pais,
    id_producto,
    nombre,
    AVG(ingreso_usd) AS promedio
  FROM `mm-tse-latam-interviews.challange_marcelo.v_mensual_producto_pais`
  GROUP BY pais, id_producto, nombre
),
spread AS (
  SELECT
    id_producto,
    nombre,
    MAX(promedio) - MIN(promedio) AS gap
  FROM resumen
  GROUP BY id_producto, nombre
)
SELECT *
FROM spread
ORDER BY gap DESC
LIMIT 5;
```

---


