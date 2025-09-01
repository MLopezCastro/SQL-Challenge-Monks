
---

# SQL Challenge – Media.Monks

**Parte 2 – Insights (Ranking, Estabilidad y Diferencias por país)**

**Dataset:** `mm-tse-latam-interviews.challange_marcelo`

**Tablas/Vistas usadas:** `ventas_limpia`, `productos`, `tdc`

**Horizonte:** enero–marzo 2022 (AR, BR, MX)

---

## 🎯 Objetivo

1. Obtener el **ranking de productos por ingresos en USD**, por mes y país.
2. Identificar el producto más **estable** en cada país.
3. Detectar el producto con mayores **diferencias de consumo entre países**.

---

## 💵 Paso 1 – Conversión a USD

El ingreso en dólares se calcula como:

$$
ingreso\_usd = cantidad \times \frac{precio\_moneda\_local}{tdc}
$$

**Vista en BigQuery**

```sql
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd` AS
SELECT
  v.id_venta,
  v.creation_date,
  v.pais,
  v.id_producto,
  p.nombre,
  p.categoria,
  v.cantidad,
  v.precio_moneda_local,
  t.tdc,
  SAFE_DIVIDE(v.precio_moneda_local, t.tdc) AS precio_usd,
  v.cantidad * SAFE_DIVIDE(v.precio_moneda_local, t.tdc) AS ingreso_usd
FROM `mm-tse-latam-interviews.challange_marcelo.ventas_limpia` v
JOIN `mm-tse-latam-interviews.challange_marcelo.tdc` t
  ON t.pais = v.pais AND t.fecha_tdc = v.creation_date
JOIN `mm-tse-latam-interviews.challange_marcelo.productos` p
  ON CAST(p.id_producto AS STRING) = v.id_producto;
```

---

## 🏆 Paso 2 – Ranking mensual por país

Se calcula el ingreso USD por producto, mes y país, y se asigna un ranking.

```sql
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_ranking_mensual` AS
WITH mensual AS (
  SELECT
    DATE_TRUNC(creation_date, MONTH) AS mes,
    pais,
    id_producto,
    nombre,
    SUM(ingreso_usd) AS ingreso_usd
  FROM `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd`
  GROUP BY 1,2,3,4
)
SELECT
  mes,
  pais,
  id_producto,
  nombre,
  ingreso_usd,
  DENSE_RANK() OVER (PARTITION BY pais, mes ORDER BY ingreso_usd DESC) AS rk
FROM mensual;
```

**Uso**

```sql
SELECT *
FROM `mm-tse-latam-interviews.challange_marcelo.v_ranking_mensual`
WHERE rk <= 5
ORDER BY pais, mes, rk;
```

---

## 📈 Paso 3 – Estabilidad de productos

La estabilidad se midió con el **coeficiente de variación (CV)** del ingreso USD mensual por producto y país.

```sql
WITH mensual AS (
  SELECT
    DATE_TRUNC(creation_date, MONTH) AS mes,
    pais,
    id_producto,
    nombre,
    SUM(ingreso_usd) AS ingreso_usd
  FROM `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd`
  GROUP BY 1,2,3,4
),
agg AS (
  SELECT
    pais,
    id_producto,
    nombre,
    COUNT(*) AS meses_con_venta,
    AVG(ingreso_usd) AS avg_usd_mes,
    STDDEV_SAMP(ingreso_usd) AS sd_usd_mes,
    SAFE_DIVIDE(STDDEV_SAMP(ingreso_usd), NULLIF(AVG(ingreso_usd),0)) AS cv
  FROM mensual
  GROUP BY 1,2,3
)
SELECT pais, id_producto, nombre, avg_usd_mes, cv
FROM agg
WHERE meses_con_venta >= 2
QUALIFY ROW_NUMBER() OVER (PARTITION BY pais ORDER BY cv ASC, avg_usd_mes DESC) = 1;
```

👉 **Respuesta esperada:** este query devuelve el producto más **estable** en cada país.

---

## 🌎 Paso 4 – Diferencias de consumo entre países

Se comparan los promedios mensuales en USD por producto, calculando el **rango (máx − mín)**.

```sql
WITH avg_por_pais AS (
  SELECT
    id_producto,
    nombre,
    pais,
    AVG(ingreso_usd) AS avg_usd_mes
  FROM `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd`
  GROUP BY 1,2,3
),
spread AS (
  SELECT
    id_producto,
    nombre,
    MAX(avg_usd_mes) AS max_avg,
    MIN(avg_usd_mes) AS min_avg,
    (MAX(avg_usd_mes) - MIN(avg_usd_mes)) AS rango_usd,
    (SELECT pais FROM UNNEST(ARRAY_AGG(STRUCT(pais, avg_usd_mes) ORDER BY avg_usd_mes DESC)) LIMIT 1).pais AS pais_top,
    (SELECT pais FROM UNNEST(ARRAY_AGG(STRUCT(pais, avg_usd_mes) ORDER BY avg_usd_mes ASC)) LIMIT 1).pais AS pais_bottom
  FROM avg_por_pais
  GROUP BY 1,2
)
SELECT *
FROM spread
ORDER BY rango_usd DESC
LIMIT 1;
```

👉 **Respuesta esperada:** este query devuelve el producto con mayor diferencia de consumo entre países, junto con el país donde más se vendió y el país donde menos.

---

## ✅ Conclusiones – Parte 2

* **Ranking mensual:** generado en la vista `v_ranking_mensual`, permite ver el top de productos por país y mes.
* **Estabilidad:** usando el CV, se identificó el producto más estable en cada país (ventas más uniformes entre meses).
* **Diferencias entre países:** se detectó el producto con mayor heterogeneidad de consumo, destacando el país donde más se consume vs. donde menos.

Estos hallazgos serán insumo para la **Parte 3 (Looker Studio)**, donde se crearán visualizaciones interactivas.

---


