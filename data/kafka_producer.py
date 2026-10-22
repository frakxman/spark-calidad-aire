#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Productor de Kafka: simula la llegada en tiempo real de las mediciones de
calidad del aire, reenviando una fila por segundo del archivo
resultados/datos_limpios.csv al topic 'calidad_aire'.

Ejecución:
    python3 kafka_producer.py        (detener con Ctrl+C)
"""

import csv
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

SERVIDOR = "localhost:9092"
TOPIC = "calidad_aire"
SEGUNDOS_ENTRE_MENSAJES = 1
CAMPOS = ["CO(GT)", "C6H6(GT)", "NOx(GT)", "NO2(GT)", "T", "RH"]


def ruta_datos():
    carpeta = Path(__file__).resolve().parent
    raiz = carpeta.parent if carpeta.name == "data" else carpeta
    ruta = raiz / "resultados" / "datos_limpios.csv"
    if not ruta.is_file():
        raise FileNotFoundError(
            "No existe %s. Ejecuta primero spark-submit analisis_calidad_aire.py" % ruta
        )
    return ruta


def numero(texto):
    texto = (texto or "").strip()
    return float(texto) if texto else None


def leer_mediciones(ruta):
    """Filas con CO, NOx y NO2 presentes, para que el flujo sea limpio."""
    mediciones = []
    with open(ruta, encoding="utf-8", newline="") as f:
        for fila in csv.DictReader(f, delimiter=";"):
            valores = {campo: numero(fila[campo]) for campo in CAMPOS}
            if None in (valores["CO(GT)"], valores["NOx(GT)"], valores["NO2(GT)"]):
                continue
            valores["fecha_original"] = fila["FechaHora"]
            mediciones.append(valores)
    return mediciones


def main():
    productor = None
    enviados = 0
    try:
        mediciones = leer_mediciones(ruta_datos())
        print("Mediciones cargadas: %d" % len(mediciones))
        productor = KafkaProducer(
            bootstrap_servers=[SERVIDOR],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        while True:
            for medicion in mediciones:
                productor.send(TOPIC, value=medicion)
                enviados += 1
                print("Enviado #%d: %s" % (enviados, medicion))
                time.sleep(SEGUNDOS_ENTRE_MENSAJES)
    except KeyboardInterrupt:
        print("\nProductor detenido. Mensajes enviados: %d" % enviados)
        if productor is not None:
            productor.flush()
        return 0
    except Exception as exc:
        print("ERROR: %s: %s" % (type(exc).__name__, exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
