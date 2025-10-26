from flask import Flask, jsonify
from flask_cors import CORS
from utils.drive_utils import obtener_ultimo_snapshot
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sys
sys.stdout.reconfigure(encoding='utf-8')

app = Flask(__name__)
CORS(app)


# ========================
#        HOME
# ========================
@app.route("/")
def home():
    return jsonify({"status": "ok", "message": "Backend funcionando correctamente"})


# ========================
#    INCIDENCIAS DETALLE
# ========================
@app.route("/incidencias")
def incidencias():
    ruta_local = "data/snapshot_actual.json.gz"
    os.makedirs("data", exist_ok=True)
    df = obtener_ultimo_snapshot(ruta_local)

    if "N° Tienda" not in df.columns:
        return jsonify({"error": "No se encuentra la columna 'N° Tienda' en el snapshot"}), 400
    df["N° Tienda"] = df["N° Tienda"].astype(str).str.strip()

    if "Departamento" not in df.columns:
        df["Departamento"] = "0"
    else:
        df["Departamento"] = df["Departamento"].fillna("0").replace("", "0")

    # --- Catálogos ---
    df_tiendas = pd.read_csv("data/tiendas.csv", dtype={"Tienda": str})
    df_deptos = pd.read_csv("data/departamentos.csv", dtype={"Departamento_ID": str})

    # --- Merge Tiendas ---
    df = df.merge(df_tiendas, how="left", left_on="N° Tienda", right_on="Tienda")
    for col in ["Distrito", "Distrital", "Formato", "Nombre", "Provincia", "Direccion"]:
        if f"{col}_y" in df.columns:
            df[col] = df[f"{col}_y"]
        elif f"{col}_x" in df.columns:
            df[col] = df[f"{col}_x"]
        df[col] = df[col].fillna("Otro")

    # --- Merge Departamentos ---
    df = df.merge(df_deptos, how="left", left_on="Departamento", right_on="Departamento_ID")
    df["Departamento_DESC"] = df["Departamento_DESC"].fillna("Otro")
    df["Coordinacion"] = df["Coordinacion"].fillna("Otro")
    df["Area"] = df["Area"].fillna("Otro")

    # --- Fechas ---
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

    df["Departamento"] = pd.to_numeric(df["Departamento"], errors="coerce").fillna(0).astype(int)
    df["Reincidencia"] = pd.to_numeric(df["Reincidencia"], errors="coerce").fillna(0).astype(int)

    # --- Correcciones por estado ---
    now = datetime.now()
    for i, row in df.iterrows():
        estado = str(row.get("Estado", "")).strip()
        if pd.isna(row["Inicio Status"]):
            df.at[i, "Inicio Status"] = row["Marca temporal"] + timedelta(hours=1) if pd.notna(row["Marca temporal"]) else now
        if estado == "Finalizado" and pd.isna(row["Fecha de resolucion"]):
            df.at[i, "Fecha de resolucion"] = df.at[i, "Inicio Status"] + timedelta(hours=1)
        elif estado == "Error" and pd.isna(row["Fecha de resolucion"]):
            df.at[i, "Fecha de resolucion"] = row["Marca temporal"] + timedelta(hours=1) if pd.notna(row["Marca temporal"]) else now

    # --- Respuesta por incidencia ---
    if "Sector Respuesta" in df.columns:
        df["Respuesta por incidencia"] = np.where(
            df["Sector Respuesta"].str.lower().fillna("").str.contains("incidencia"),
            "Equipo Incidencias",
            "Derivada a grupo resolutor"
        )
    else:
        df["Respuesta por incidencia"] = "Derivada a grupo resolutor"

    # --- Cálculos de tiempos ---
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

    df["Recuento"] = 1
    muestra = df.head(30).fillna("").to_dict(orient="records")

    return jsonify({
        "total": len(df),
        "columnas": list(df.columns),
        "muestra": muestra
    })


# ========================
#    INCIDENCIAS RESUMEN
# ========================
@app.route("/incidencias/resumen")
def incidencias_resumen():
    try:
        ruta_local = "data/snapshot_actual.json.gz"
        df = obtener_ultimo_snapshot(ruta_local)
        print(f"✅ Snapshot cargado: {len(df)} filas")

        if "Marca temporal" not in df.columns:
            return jsonify({"error": "Columna 'Marca temporal' no encontrada"}), 400

        df["Marca temporal"] = pd.to_datetime(df["Marca temporal"], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["Marca temporal"])
        df["Recuento"] = 1

        # --- Grupo resolutor ---
        def grupo_resolutor(row):
            inc = str(row.get("N° Incidente", "")).upper()
            if inc.startswith("CD"):
                return "Centro de Distribución"
            elif inc.startswith("C"):
                return "Compras"
            elif inc.startswith("R"):
                return "Reabastecimiento"
            elif inc.startswith(("M", "P")):
                try:
                    num = int(inc.split("-")[1])
                    return "Compras" if num > 100 else "Mkt / ISM / Pricing"
                except:
                    return "Mkt / ISM / Pricing"
            else:
                return "Otro"

        df["Grupo resolutor"] = df.apply(grupo_resolutor, axis=1)

        # --- Agrupar ---
        def agrupar_por(periodo):
            df_temp = df.copy()
            if periodo == "fecha":
                df_temp["Periodo"] = df_temp["Marca temporal"].dt.strftime("%d-%m")
            elif periodo == "semana":
                df_temp["Periodo"] = "W" + df_temp["Marca temporal"].dt.isocalendar().week.astype(str) + "-" + df_temp["Marca temporal"].dt.strftime("%y")
            else:
                df_temp["Periodo"] = df_temp["Marca temporal"].dt.strftime("%b-%y")

            agg = (
                df_temp.groupby(["Periodo", "Grupo resolutor"])["Recuento"]
                .sum()
                .reset_index()
                .sort_values(by="Periodo")
            )
            return agg

        resumen = {
            "por_fecha": agrupar_por("fecha").to_dict(orient="records"),
            "por_semana": agrupar_por("semana").to_dict(orient="records"),
            "por_mes": agrupar_por("mes").to_dict(orient="records"),
            "por_estado": df["Estado"].value_counts().to_dict(),
            "total": len(df)
        }

        # --- Promedios de horas por estado ---
        if "Horas en estado" in df.columns:
            resumen["promedios_horas"] = df.groupby("Estado")["Horas en estado"].mean().round(1).to_dict()
        else:
            resumen["promedios_horas"] = {}

        # --- Filtros ---
        df_tiendas = pd.read_csv("data/tiendas.csv", dtype=str)
        df_deptos = pd.read_csv("data/departamentos.csv", dtype=str)
        resumen["filtros"] = {
            "Departamentos": sorted(df_deptos["Departamento_DESC"].dropna().unique().tolist()),
            "Distritos": sorted(df_tiendas["Distrito"].dropna().unique().tolist()),
            "Provincias": sorted(df_tiendas["Provincia"].dropna().unique().tolist()),
            "Tiendas": sorted([f"{row.Tienda} - {row.Nombre}" for _, row in df_tiendas.iterrows()])
        }

        print("✅ Resumen generado correctamente.")
        return jsonify(resumen)

    except Exception as e:
        print(f"❌ Error en incidencias_resumen: {e}")
        return jsonify({"error": str(e)}), 500


# ========================
#         MAIN
# ========================
if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    app.run(debug=True)
