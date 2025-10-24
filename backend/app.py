from flask import Flask, jsonify
from utils.drive_utils import obtener_ultimo_snapshot
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return jsonify({"status": "ok", "message": "Backend funcionando correctamente"})

@app.route("/incidencias")
def incidencias():
    ruta_local = "data/snapshot_actual.json.gz"
    os.makedirs("data", exist_ok=True)

    # === Cargar snapshot ===
    df = obtener_ultimo_snapshot(ruta_local)

    # === Normalización base ===
    if "N° Tienda" not in df.columns:
        return jsonify({"error": "No se encuentra la columna 'N° Tienda' en el snapshot"}), 400
    df["N° Tienda"] = df["N° Tienda"].astype(str).str.strip()

    # Crear columna Departamento si no existe
    if "Departamento" not in df.columns:
        df["Departamento"] = "0"
    else:
        df["Departamento"] = df["Departamento"].fillna("0").replace("", "0")

    # === Cargar catálogos ===
    df_tiendas = pd.read_csv("data/tiendas.csv", dtype={"Tienda": str})
    df_deptos = pd.read_csv("data/departamentos.csv", dtype={"Departamento_ID": str})

    # === Merge con Tiendas ===
    df = df.merge(df_tiendas, how="left", left_on="N° Tienda", right_on="Tienda")

    # Asegurar columnas tras merge (pandas puede duplicar)
    for col in ["Distrito", "Distrital", "Formato", "Nombre", "Provincia", "Direccion"]:
        if f"{col}_y" in df.columns:
            df[col] = df[f"{col}_y"]
        elif f"{col}_x" in df.columns:
            df[col] = df[f"{col}_x"]
        df[col] = df[col].fillna("Otro")

    # === Merge con Departamentos ===
    if "Departamento" not in df.columns:
        df["Departamento"] = "0"

    df = df.merge(df_deptos, how="left", left_on="Departamento", right_on="Departamento_ID")
    df["Departamento_DESC"] = df["Departamento_DESC"].fillna("Otro")
    df["Coordinacion"] = df["Coordinacion"].fillna("Otro")
    df["Area"] = df["Area"].fillna("Otro")

    # === Conversión de tipos ===
    def parse_dt(x):
        try:
            return pd.to_datetime(x, errors="coerce", dayfirst=True)
        except:
            return pd.NaT

    for col in ["Marca temporal", "Inicio Status", "Fecha de resolucion"]:
        if col in df.columns:
            df[col] = df[col].apply(parse_dt)

    if "Vencimiento" in df.columns:
        df["Vencimiento"] = pd.to_datetime(df["Vencimiento"], errors="coerce", dayfirst=True).dt.date

    # Departamentos y Reincidencia
    df["Departamento"] = pd.to_numeric(df["Departamento"], errors="coerce").fillna(0).astype(int)
    df["Reincidencia"] = pd.to_numeric(df["Reincidencia"], errors="coerce").fillna(0).astype(int)

    # === Correcciones de fechas según estado ===
    now = datetime.now()

    for i, row in df.iterrows():
        estado = str(row.get("Estado", "")).strip()
        if pd.isna(row["Inicio Status"]):
            df.at[i, "Inicio Status"] = row["Marca temporal"] + timedelta(hours=1) if pd.notna(row["Marca temporal"]) else now
        if estado == "Finalizado" and pd.isna(row["Fecha de resolucion"]):
            df.at[i, "Fecha de resolucion"] = df.at[i, "Inicio Status"] + timedelta(hours=1)
        elif estado == "Error" and pd.isna(row["Fecha de resolucion"]):
            df.at[i, "Fecha de resolucion"] = row["Marca temporal"] + timedelta(hours=1) if pd.notna(row["Marca temporal"]) else now

    # === Respuesta por incidencia ===
    if "Sector Respuesta" in df.columns:
        df["Respuesta por incidencia"] = np.where(
            df["Sector Respuesta"].str.lower().fillna("").str.contains("incidencia"),
            "Equipo Incidencias",
            "Derivada a grupo resolutor"
        )
    else:
        df["Respuesta por incidencia"] = "Derivada a grupo resolutor"

    # === Cálculos de tiempos ===
    df["Horas en estado"] = np.nan
    df["Tiempo Promedio Pendiente"] = np.nan
    df["Tiempo Promedio En Proceso"] = np.nan

    for i, row in df.iterrows():
        estado = str(row.get("Estado", "")).strip()
        marca, inicio, resol = row.get("Marca temporal"), row.get("Inicio Status"), row.get("Fecha de resolucion")

        if estado == "Pendiente" and pd.notna(marca):
            df.at[i, "Horas en estado"] = (now - marca).total_seconds() / 3600
        elif estado == "En Proceso" and pd.notna(inicio):
            df.at[i, "Horas en estado"] = (now - inicio).total_seconds() / 3600

        if pd.notna(inicio) and pd.notna(marca):
            df.at[i, "Tiempo Promedio Pendiente"] = (inicio - marca).total_seconds() / 3600
        if pd.notna(resol) and pd.notna(inicio):
            df.at[i, "Tiempo Promedio En Proceso"] = (resol - inicio).total_seconds() / 3600

    # Campo recuento
    df["Recuento"] = 1

    # === Muestra ===
    muestra = df.head(30).fillna("").to_dict(orient="records")

    return jsonify({
        "total": len(df),
        "columnas": list(df.columns),
        "muestra": muestra
    })


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    app.run(debug=True)
