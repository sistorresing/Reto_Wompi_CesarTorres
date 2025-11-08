-----
# 🚀 Reto Técnico – Ingeniero de Datos (Wompi)
-----



## 📄 Descripción

Este proyecto procesa un archivo **JSONL de transacciones**, llamado:  **transactions_50k.jsonl**
extrae el campo anidado `payment_method_type.extra.bin`, que contiene la informacion del "BIN"
extraemos la fecha (`day`) en formato DD-MM-YYY,
Usamos el campo **status** para filtrar transacciones **aprobadas** con (`status == "APPROVED"`)
y finalmente el script genera una **vista agregada** por **BIN** y **día** con las siguientes caracteristicas:

  * 🧮 Conteo de transacciones aprobadas → `approved_tx_count`

  * 💰 Suma total en centavos → `approved_amount_cents`

El resultado se guarda en formato **Parquet**, garantizando *idempotencia* y verificación de consistencia con las funciones (`shape` y `sum`).

## ⚙️ Requisitos

		**En una misma carpeta deben existir los siquientes archivos:
		
		* main.py
		* requirements.txt
		* transactions_50k.jsonl


**Archivo `requirements.txt`:**
 Existe y debe estar en la misma carpeta donde se ejecuta el proyecto
 
 Contiene:
				```
				pandas
				pyarrow
				```

**Instalación de dependencias:**

	🛑 Ejecute la sentencia siguiente en su terminal de Python ubicado donde se encuentre le proyecto.

```bash
py -m pip install -r requirements.txt
```

## 🚀 Ejecución Script Solucion

Ejecuta el script principal indicando archivo de entrada y salida con la sentencia:

```bash
py main.py --input transactions_50k.jsonl --output outputs/agg_transactions.parquet
```

El script lee el archivo, extrae BIN, agrega por día y genera el Parquet.

## 🧾 Salida Esperada

		```text
		======================================================================
		🎯 HITOS ESPERADOS EN LA SALIDA
		======================================================================

			* Comprobacion del alistamiento del ambiente:
			* Lectura y validación del archivo JSONL
			* Detección de columnas con estructuras anidadas o JSON en texto
			* Agregación paso de procesamiento o transformacion
			* Escritura y salida con validacion de Idempotencia: si existe el archivo, lo elimína y vuelve a crearlo

			** PROCESO FINALIZADO CON ÉXITO **
		======================================================================
		```

## 🧩 Ver el log de ejecución

Para revisar la trazabilidad de cada paso y sus tiempos y como se ejecuto,
 (Tambien para comprobar la hora de ejecucion con la fecha y hora de la salida)
  por favor ejecutar en su consola:

			```bash
			py -c "print(open('outputs/execution.log').read())"
			```

## ✅ Detalles técnicos

  * 🏍️ Procesamiento **idempotente** (sobrescribe Parquet limpio en cada ejecución)

  * 👍 Validación **shape + suma total** entre el DataFrame original y el agregado

  * 🪟 Formato **Parquet columnar**, ideal para entornos distribuidos

  * 🌍 Portable (Windows / Mac / Linux) — rutas con `pathlib`

  * ‍‍🧑‍🤝‍🧑 Compatible con Colab o ejecución local (Python 3.10+)

## 📁 Estructura del proyecto

```text
Prueba_Wompi/
├── main.py
├── requirements.txt
├── transactions_50k.jsonl
├── outputs/
│   ├── execution.log
│   └── agg_transactions.parquet
└── README.md
```

## 💬 Notas de implementación

1.  Se usa `pandas` para lectura/procesamiento del JSONL.
2.  Se extrae solo lo necesario del JSON anidado (`extra.bin`) para eficiencia.
3.  Verificaciones de idempotencia, conteo y suma total.
4.  Logging por bloque en `outputs/execution.log` con timestamps.

---
**Autor:** César Torres  
* Prueba Técnica – Ingeniero de Datos (Wompi)*  
* Noviembre 2025  
---