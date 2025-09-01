
# SQL Challenge – Media.Monks (BigQuery + Looker Studio)  

**Parte 2 – Insights (Ranking, Estabilidad y Diferencias)**

**Repo:** `SQL-Challenge-Monks`  

**Dataset origen:** `mm-tse-latam-interviews.challange_marcelo`

**Tablas trabajadas:** `ventas_limpia`, `productos`, `tdc_norm` 

**Vistas creadas:** `v_ventas_usd`, `v_mensual_producto_pais`, `v_ranking_mensual`  

---

## 🧭 Contexto

El **Ejercicio 2** pedía:  
1. Construir el **ranking mensual de productos por país** en términos de ingresos en USD.  
2. Responder:  
   - **¿Qué producto es más estable a lo largo de los meses en cada país?**  
   - **¿Qué producto muestra mayores diferencias de consumo entre países?**  

Para resolverlo:  
- Normalizamos el **tipo de cambio (TDC)** creando `tdc_norm`.  
- Construimos una vista intermedia `v_ventas_usd` para tener cada venta ya convertida a USD.  
- Agregamos datos a nivel **mes-país-producto** (`v_mensual_producto_pais`).  
- Calculamos el **ranking mensual por país** (`v_ranking_mensual`).  
- Finalmente, respondimos las dos preguntas con queries específicas de estabilidad y diferencias.

---

## 📊 Paso 1 — Normalización de TDC

Los valores de país en `tdc` venían heterogéneos (`Arg`, `Arg1`, `Bra`, `Bra2`, `Mex`, `Mex3`).  
Creamos la vista `tdc_norm` para unificarlos en **AR / BR / MX**.

```sql
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


✅ **Validación:**

* Todos los registros quedaron con país `AR`, `BR` o `MX`.
* 90 filas por país → correcto (3 meses × 30 días aprox.).

```
## 💵 Paso 2 — Vista de ventas en USD

Creamos la vista `v_ventas_usd` que une ventas limpias + productos + tipo de cambio.

```sql
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd` AS
SELECT
  v.id_venta,
  v.creation_date,
  v.pais,
  v.id_producto,
  p.NOMBRE   AS nombre,
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

✅ **Validación:**

* Filas en `v_ventas_usd`: **5,287** (coincide con ventas\_limpia).
* Ejemplo de cálculo: precio local 1000 / TDC 203.39 ≈ 4.91 USD → correcto.

---

## 📅 Paso 3 — Agregación mensual por país/producto

Vista `v_mensual_producto_pais` → ingresos en USD agrupados.

```sql
CREATE OR REPLACE VIEW `mm-tse-latam-interviews.challange_marcelo.v_mensual_producto_pais` AS
SELECT
  DATE_TRUNC(creation_date, MONTH) AS mes,
  pais,
  id_producto,
  ANY_VALUE(nombre) AS nombre,
  ANY_VALUE(categoria) AS categoria,
  SUM(cantidad) AS unidades,
  SUM(ingreso_usd) AS ingreso_usd
FROM `mm-tse-latam-interviews.challange_marcelo.v_ventas_usd`
GROUP BY 1,2,3;
```

✅ **Validación:**

* Filas: **45** (3 países × 3 meses × 5 productos).
* Ejemplo AR – enero 2022:

  * Zapatos GTV → 54,443 USD
  * Blazer fixed → 32,976 USD
  * Remera unisex → 15,696 USD
  * Piluso multix → 11,510 USD
  * Pack x 3 → 3,192 USD

---

## 🏆 Paso 4 — Ranking mensual por país

Vista `v_ranking_mensual` → asigna posición a cada producto según ingreso USD.

```sql
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

✅ **Validación:**

* Top 5 por país/mes aparecen ordenados correctamente.
* Ejemplo AR enero 2022:

  1. Zapatos GTV
  2. Blazer fixed
  3. Remera unisex
  4. Piluso multix
  5. Pack x 3 – Medias infantiles

---

## 🔎 Paso 5 — Respuestas a las preguntas

### Pregunta 1: Estabilidad de productos

Cálculo del coeficiente de variación (CV = Desvío / Promedio).

```sql
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
       ROUND(desv,2) AS desv_usd,
       ROUND(cv,3) AS cv
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY pais ORDER BY cv ASC) AS rn
  FROM stats
)
WHERE rn = 1
ORDER BY pais;
```

✅ **Resultados:**

* **AR:** Blazer fixed (CV 0.029 → muy estable)
* **BR:** Pack x 3 – Medias infantiles (CV 0.072)
* **MX:** Remera unisex (CV 0.056)

📌 **Interpretación:**
Estos productos fueron los más **consistentes mes a mes** en cada país, manteniendo ingresos similares en enero, febrero y marzo.

---

### Pregunta 2: Diferencias entre países

Cálculo del “gap” = diferencia entre máximo y mínimo promedio por producto.

```sql
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

✅ **Resultados (top 5):**

1. Blazer fixed → gap ≈ 3,962 USD
2. Remera unisex → gap ≈ 1,784 USD
3. Zapatos GTV → gap ≈ 1,412 USD
4. Piluso multix → gap ≈ 1,027 USD
5. Pack x 3 – Medias infantiles → gap ≈ 250 USD

📌 **Interpretación:**
El producto con mayor **diferencia entre países** es **Blazer fixed**: en algunos mercados se vende mucho más que en otros. Esto lo convierte en el más “desparejo” en su distribución geográfica.

---

## ✅ Conclusión

En este segundo bloque logramos:

* Generar un **ranking mensual por país** en USD.
* Identificar los productos más **estables** (ventas parejas mes a mes).
* Detectar el producto con mayores **diferencias entre países**.

Esto da un panorama claro para el equipo de negocio:

* Saber qué productos tienen **ingresos constantes y predecibles**.
* Detectar **oportunidades de expansión** (productos con grandes diferencias entre países).

---

```

¿Querés que ahora preparemos el **Parte 3 (Looker Studio)** con este mismo formato monobloque?
```
