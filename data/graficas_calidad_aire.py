#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gráficas de los resultados del análisis batch de calidad del aire.

Lee los CSV que genera analisis_calidad_aire.py (carpeta resultados/) y guarda
las gráficas como PNG en resultados/graficas/.

Ejecución:
    python3 graficas_calidad_aire.py

Requiere: matplotlib  (pip install matplotlib)
"""

import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # sin pantalla: funciona por SSH/PuTTY
import matplotlib.pyplot as plt  # noqa: E402


def resolver_carpetas():
    script_dir = Path(__file__).resolve().parent
    raiz = script_dir.parent if script_dir.name == "data" else script_dir
    resultados = raiz / "resultados"
    if not resultados.is_dir():
        raise FileNotFoundError(
            "No existe %s. Ejecuta primero spark-submit analisis_calidad_aire.py"
            % resultados
        )
    salida = resultados / "graficas"
    salida.mkdir(exist_ok=True)
    return resultados, salida


def numero(texto):
    texto = (texto or "").strip()
    return float(texto) if texto else None


def leer(ruta):
    if not ruta.is_file():
        raise FileNotFoundError("Falta el archivo %s" % ruta)
    with open(ruta, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


def guardar(fig, ruta):
    fig.tight_layout()
    fig.savefig(ruta, dpi=150)
    plt.close(fig)
    print("  OK  %s" % ruta)


def grafica_por_hora(resultados, salida):
    filas = leer(resultados / "promedios_por_hora.csv")
    horas = [int(f["Hora"]) for f in filas]
    co = [numero(f["CO(GT)"]) for f in filas]
    nox = [numero(f["NOx(GT)"]) for f in filas]

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.plot(horas, co, color="tab:blue", marker="o", label="CO(GT)")
    ax1.set_xlabel("Hora del día")
    ax1.set_ylabel("CO(GT) promedio (mg/m³)", color="tab:blue")
    ax1.set_xticks(range(0, 24, 2))
    ax1.grid(alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(horas, nox, color="tab:red", marker="s", label="NOx(GT)")
    ax2.set_ylabel("NOx(GT) promedio (ppb)", color="tab:red")
    ax1.set_title("CO y NOx promedio por hora del día")
    guardar(fig, salida / "promedios_por_hora.png")


def grafica_mensual(resultados, salida):
    filas = leer(resultados / "promedios_mensuales.csv")
    etiquetas = ["%s-%02d" % (f["Anio"], int(f["Mes"])) for f in filas]
    co = [numero(f["CO(GT)"]) for f in filas]
    nox = [numero(f["NOx(GT)"]) for f in filas]
    x = range(len(etiquetas))

    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(x, nox, color="tab:red", alpha=0.6, label="NOx(GT)")
    ax1.set_ylabel("NOx(GT) promedio (ppb)", color="tab:red")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(etiquetas, rotation=45, ha="right")
    ax2 = ax1.twinx()
    ax2.plot(x, co, color="tab:blue", marker="o", label="CO(GT)")
    ax2.set_ylabel("CO(GT) promedio (mg/m³)", color="tab:blue")
    ax1.set_title("Promedios mensuales de NOx y CO")
    ax1.text(
        0.99, 0.97, "Abril de 2005 es un mes incompleto (87 registros)",
        transform=ax1.transAxes, ha="right", va="top", fontsize=8, color="gray",
    )
    guardar(fig, salida / "promedios_mensuales.png")


def grafica_correlaciones(resultados, salida):
    filas = [
        f for f in leer(resultados / "correlaciones_nox.csv")
        if f["Correlacion_con_NOx"].strip()
    ]
    if not filas:
        print("  AVISO: no hay correlaciones calculadas; se omite la gráfica.")
        return
    filas.sort(key=lambda f: float(f["Correlacion_con_NOx"]))
    variables = [f["Variable"] for f in filas]
    valores = [float(f["Correlacion_con_NOx"]) for f in filas]
    colores = ["tab:red" if v < 0 else "tab:green" for v in valores]

    fig, ax = plt.subplots(figsize=(8, 5))
    barras = ax.barh(variables, valores, color=colores)
    ax.bar_label(barras, fmt="%.2f", padding=3)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlim(-1, 1)
    ax.set_xlabel("Coeficiente de correlación de Pearson")
    ax.set_title("Correlación de cada variable con NOx(GT)")
    guardar(fig, salida / "correlaciones_nox.png")


def grafica_ausentes(resultados, salida):
    filas = leer(resultados / "valores_ausentes.csv")
    filas.sort(key=lambda f: float(f["Porcentaje_ausentes"]))
    variables = [f["Variable"] for f in filas]
    porcentajes = [float(f["Porcentaje_ausentes"]) for f in filas]

    fig, ax = plt.subplots(figsize=(8, 6))
    barras = ax.barh(variables, porcentajes, color="tab:orange")
    ax.bar_label(barras, fmt="%.1f %%", padding=3)
    ax.set_xlim(0, max(porcentajes) * 1.15 if porcentajes else 1)
    ax.set_xlabel("Valores ausentes (%)")
    ax.set_title("Porcentaje de valores ausentes por variable")
    guardar(fig, salida / "valores_ausentes.png")


def main():
    try:
        resultados, salida = resolver_carpetas()
        print("Leyendo: %s" % resultados)
        grafica_por_hora(resultados, salida)
        grafica_mensual(resultados, salida)
        grafica_correlaciones(resultados, salida)
        grafica_ausentes(resultados, salida)
        print("\nGráficas generadas en: %s" % salida)
        return 0
    except Exception as exc:
        print("ERROR: %s: %s" % (type(exc).__name__, exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
