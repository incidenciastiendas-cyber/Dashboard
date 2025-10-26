import os
import pandas as pd
import gzip
import json

def obtener_ultimo_snapshot(ruta_local):
    """
    Carga el snapshot local si existe.
    Si no existe, lanza un error informativo (no intenta usar Google Drive).
    """
    os.makedirs(os.path.dirname(ruta_local), exist_ok=True)

    if os.path.exists(ruta_local):
        print("✅ Usando snapshot local en caché.")
        try:
            df = pd.read_json(ruta_local, compression="gzip")
            print(f"📊 Snapshot cargado: {len(df)} filas.")
            return df
        except Exception as e:
            raise RuntimeError(f"Error al leer el snapshot local: {e}")

    # Si no existe el archivo, informar claramente
    raise FileNotFoundError(
        f"❌ No se encontró el snapshot local: {ruta_local}. "
        "Coloca el archivo 'snapshot_actual.json.gz' en la carpeta 'data/'."
    )
