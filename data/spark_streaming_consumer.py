#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consumidor de Spark Structured Streaming.

Lee del topic 'calidad_aire' de Kafka, interpreta los mensajes JSON y calcula,
por ventanas de 1 minuto, el número de mediciones y los promedios de CO, NO2,
NOx y temperatura. Los resultados se muestran en la consola.

Ejecución (primero el productor, luego este programa):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
        spark_streaming_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SERVIDOR = "localhost:9092"
TOPIC = "calidad_aire"
VENTANA = "1 minute"
INTERVALO = "10 seconds"

ESQUEMA = StructType([
    StructField("CO(GT)", DoubleType()),
    StructField("C6H6(GT)", DoubleType()),
    StructField("NOx(GT)", DoubleType()),
    StructField("NO2(GT)", DoubleType()),
    StructField("T", DoubleType()),
    StructField("RH", DoubleType()),
    StructField("fecha_original", StringType()),
])


def interpretar_mensajes(df):
    """Convierte el valor JSON en columnas con nombres simples.

    Los nombres con paréntesis se referencian con backticks.
    'timestamp' es la hora de llegada del mensaje a Kafka.
    """
    datos = F.from_json(F.col("value").cast("string"), ESQUEMA).alias("d")
    return df.select(F.col("timestamp"), datos).select(
        F.col("timestamp"),
        F.col("d.`CO(GT)`").alias("co"),
        F.col("d.`NOx(GT)`").alias("nox"),
        F.col("d.`NO2(GT)`").alias("no2"),
        F.col("d.`T`").alias("temperatura"),
    )


def estadisticas_por_ventana(df):
    return (
        df.groupBy(F.window(F.col("timestamp"), VENTANA))
        .agg(
            F.count(F.lit(1)).alias("mediciones"),
            F.round(F.avg("co"), 2).alias("CO_prom"),
            F.round(F.avg("no2"), 2).alias("NO2_prom"),
            F.round(F.avg("nox"), 2).alias("NOx_prom"),
            F.round(F.max("nox"), 2).alias("NOx_max"),
            F.round(F.avg("temperatura"), 2).alias("T_prom"),
        )
        .orderBy(F.col("window").desc())
    )


def main():
    spark = (
        SparkSession.builder.appName("CalidadAireStreaming")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", SERVIDOR)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    estadisticas = estadisticas_por_ventana(interpretar_mensajes(crudo))

    consulta = (
        estadisticas.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime=INTERVALO)
        .start()
    )
    consulta.awaitTermination()


if __name__ == "__main__":
    main()
