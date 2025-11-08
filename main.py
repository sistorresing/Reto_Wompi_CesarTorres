# -*- coding: utf-8 -*-

# ###############################################
#  Nombre:  main.py                             #
#  Proyecto: Reto_Practico_Wompi                #
#  Fecha:  07/11/2025                           #
#  Elaborado por :  Cesar Augusto Torres        #
#  Objetivo:                                    #
#      - Lee el archivo de entrada 		        #
#			 - procesa las transacciones        #
#			 - genera una vista agregada        #
#			   con las diferentes métricas      #
#			   solicitadas                      #
#      - Guardar Salida en formato Parquet      #
#  Version: 1.0                                 #
#  Equipo:  Prueba Técnica Ingeniero de Datos	#
#			      Wompi 					    #
# ###############################################


#1. Preparacion del Ambiente  Imports - Definicion de Rutas.

import datetime, time, os
import pandas as pd
from pathlib import Path


start = time.time()


from pathlib import Path
import datetime, time, os
import pandas as pd


print("\n \n Ejecucion Prueba Tecnica - Ingeniero de Datos  \n Parte 2 - Reto Practico \n \n")



BASE_DIR   = Path(__file__).parent.resolve()   # ← ancla al folder del script
INPUT_PATH = BASE_DIR / "transactions_50k.jsonl"
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_FILE = OUTPUT_DIR / "agg_transactions.parquet"


OUTPUT_DIR.mkdir(exist_ok=True)  # Asegura la posibilidad de ejecutar y siempre poder validar si la carpeta existe o no para crearla.

# Comprobacion del alistamiento del ambiente:
print("\n Comprobacion del alistamiento del ambiente: \n")

print("INPUT_PATH :", INPUT_PATH)
print("OUTPUT_DIR :", OUTPUT_DIR.resolve())
print("OUTPUT_FILE:", OUTPUT_FILE.resolve())
print("Entorno listo")


#Control
log_path = os.path.join("outputs", "execution.log")
open(os.path.join("outputs","execution.log"), "a").write(f"\n----- Nueva Ejecucion -----\n{datetime.datetime.now()} | Preparacion de Ambiente Realizado en:{round(time.time()-start,2)}s\n")

# Lectura y validación del archivo JSONL

print("\n Lectura y validación del archivo JSONL \n")

start = time.time()

# Lectura y validaciones básicas
df = pd.read_json(INPUT_PATH, lines=True)
print(f"Cantidad de Filas Origen: {len(df):,}\n Columnas: {list(df.columns)}\n")
# print("\nTipos de datos:\n", df.dtypes)
print("\nValores únicos en 'status':", df['status'].unique()[:10])

# Detección de columnas con estructuras anidadas o JSON en texto

print("\n Detección de columnas con estructuras anidadas o JSON en texto \n")


nested_cols, json_str_cols, detail = [], [], {}

for c in df.columns:
    s = df[c].dropna().head(200)  # muestra rápida
    has_nested = any(isinstance(v, (dict, list)) for v in s)
    has_json_str = any(isinstance(v, str) and v.strip()[:1] in "{[" for v in s)

    if has_nested:
        nested_cols.append(c)
        level1, sub2 = set(), {}
        # inspección ligera de claves
        for v in s:
            if isinstance(v, dict):
                level1.update(v.keys())
                for k, val in v.items():
                    if isinstance(val, (dict, list)) and k not in sub2:
                        sub2[k] = type(val).__name__
        detail[c] = {"nivel1": sorted(level1), "subniveles": sub2}

    elif has_json_str:
        json_str_cols.append(c)

print("\nPosibles columnas ANIDADAS (dict/list):", nested_cols or "ninguna")
print("\nPosibles columnas JSON en TEXTO     :", json_str_cols or "ninguna")

for c in nested_cols:
    print(f"\n[{c}] claves nivel 1: {detail[c]['nivel1']}")
    if detail[c]["subniveles"]:
        print(f"Subniveles detectados en {c}: {detail[c]['subniveles']}")
        # muestra de claves internas por cada subnivel dict
        s = df[c].dropna().head(50)
        for subk in detail[c]["subniveles"].keys():
            inner_keys = set()
            for v in s:
                if isinstance(v, dict) and isinstance(v.get(subk), dict):
                    inner_keys.update(v[subk].keys())
            print(f"  └── {subk} contiene claves: {sorted(inner_keys)}")


#Control
log_path = os.path.join("outputs", "execution.log")
open(log_path, "a").write(f"{datetime.datetime.now()} | Lectura JSONL Realizado en: {round(time.time()-start,2)}s\n")

# Agregación paso de procesamiento o transformacion.


print("\n Agregación paso de procesamiento o transformacion \n")

start = time.time()

# hallamos el BIN dentro de la columna que lo contiene para ello normalizamos el campo completo

pm = pd.json_normalize(df['payment_method_type'])

# Selecciono la columna que contiene el BIN Sabemos (por análisis previo) que está dentro de 'extra.bin' dento de payment_method_type
bin_col = 'extra.bin'
df['bin'] = pm[bin_col].astype(str)

# Encuentro 'Dia' (fecha sin hora) desde 'created_at'
df['day'] = pd.to_datetime(df['created_at']).dt.date.astype(str)


# Filtrar las transacciones aprobadas y agregar seguna la instruccion del reto

APROBADO= "APPROVED"
df_filtrado = df[df['status'] == APROBADO]

agg = (
    df_filtrado
    .groupby(['bin', 'day'], as_index=False, dropna=False)
    .agg(
        approved_tx_count=('id', 'count'),
        approved_amount_cents=('amount_in_cents', 'sum')
    )
    .sort_values(['bin', 'day'])
)


#  Verificación

print("\n Muestra de Estructura agregada: \n", agg.head(5))
print("\n Registros agregados:", len(agg))

# Validacion de Totales para garantizar que los valores coincidan con la fuente filtrada.

print("\n Suma total original Amount:", df_filtrado['amount_in_cents'].sum())
print("\n Suma total agregada Amount:", agg['approved_amount_cents'].sum())


# Control
open(os.path.join("outputs","execution.log"), "a").write(
    f"{datetime.datetime.now()} | Agregación realizada en: {round(time.time()-start,2)}s\n"
)

start = time.time()

# Idempotencia: si existe el archivo, elimínalo y vuelve a crearlo


print("\n Escritura y salida con validacion de Idempotencia: si existe el archivo, lo elimína y vuelve a crearlo \n")


try:
    OUTPUT_FILE.unlink(missing_ok=True)
except Exception:
    pass

#  Escribir ern formato parquet como em pide el requerimiento.

agg.to_parquet(OUTPUT_FILE, index=False)

# Estoy verificando que el archivo Parquet tenga la misma cantidad de filas y columnas que el DataFrame que cree en la vista
# asi para asegurar que la escritura y lectura no alteraron el resultasdo y una validacion frente a la suma total)

agg_back = pd.read_parquet(OUTPUT_FILE)
ok_shape = agg_back.shape == agg.shape
ok_sum   = agg_back['approved_amount_cents'].sum() == agg['approved_amount_cents'].sum()

print("\n Parquet escrito en:", OUTPUT_FILE)
print("\n (Shape) origen vs destino esta correcto?:", ok_shape, "| Suma total origen vs destino ok?:", ok_sum)

# Validación simple de idempotencia: confirma que el archivo fue regenerado correctamente (fecha y tamaño actualizados)

print(f"\n Validacion de Nuevo archivo de salida: {OUTPUT_FILE} | Fecha: {datetime.datetime.fromtimestamp(os.path.getmtime(OUTPUT_FILE))} | Tamaño: {round(os.path.getsize(OUTPUT_FILE)/1024/1024,3)} MB")


# Control
open(os.path.join("outputs","execution.log"), "a").write(
    f"{datetime.datetime.now()} | Creacion vista de salida y validacion final {round(time.time()-start,2)}s\n"
)

print("\n PROCESO FINALIZADO CON ÉXITO \n")
