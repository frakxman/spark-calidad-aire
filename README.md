# Análisis de calidad del aire con PySpark

Procesamiento batch del dataset Air Quality (UCI Machine Learning Repository)
con Apache Spark 3.5.8 y Python.

## Qué hace
- Carga y limpia AirQualityUCI.csv (coma decimal, -200 como dato ausente, fecha y hora).
- Resumen estadístico, promedios generales, mensuales y por hora.
- Correlaciones con NOx(GT) y porcentaje de valores ausentes.
- Exporta 7 archivos CSV a la carpeta resultados/.

## Requisitos
Java 11, Apache Spark 3.5.8 y Python 3.

## Ejecución
    cd data
    python3 -m py_compile analisis_calidad_aire.py
    spark-submit analisis_calidad_aire.py

## Dataset
https://archive.ics.uci.edu/dataset/360/air+quality
