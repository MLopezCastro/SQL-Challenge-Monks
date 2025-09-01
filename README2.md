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
