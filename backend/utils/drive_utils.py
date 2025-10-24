from google.oauth2 import service_account
from googleapiclient.discovery import build
import io, gzip, json, os, pandas as pd
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
CARPETA_ID = "1qfEaojfuDx1GFWZPbKBPFOtMDdjjJgNm"
CRED_PATH = "credenciales.json"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def crear_servicio_drive():
    creds = service_account.Credentials.from_service_account_file(CRED_PATH, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)

def archivo_reciente(path, horas=24):
    """Verifica si un archivo existe y tiene menos de X horas."""
    if not os.path.exists(path):
        return False
    mod_time = datetime.fromtimestamp(os.path.getmtime(path))
    return datetime.now() - mod_time < timedelta(hours=horas)

def obtener_ultimo_snapshot(ruta_local):
    """Descarga el snapshot si no existe o está desactualizado."""
    if archivo_reciente(ruta_local):
        print("✅ Usando snapshot local en caché.")
    else:
        print("📥 Descargando snapshot desde Google Drive...")
        service = crear_servicio_drive()
        query = f"'{CARPETA_ID}' in parents and name contains 'snapshot_actual' and trashed = false"
        results = service.files().list(q=query, orderBy="createdTime desc", pageSize=1).execute()
        files = results.get("files", [])

        if not files:
            raise FileNotFoundError("No se encontró snapshot_actual en Drive.")

        file_id = files[0]["id"]
        nombre = files[0]["name"]
        print(f"Descargando {nombre}...")

        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = request.execute()
        fh.write(downloader)
        fh.seek(0)

        os.makedirs(os.path.dirname(ruta_local), exist_ok=True)
        with open(ruta_local, "wb") as f_out:
            f_out.write(fh.read())

        print("💾 Archivo guardado en caché local.")

    # === Cargar snapshot (ya sea nuevo o cacheado) ===
    with gzip.open(ruta_local, "rt", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "ejemplo" in data:
        df = pd.DataFrame(data["ejemplo"])
    else:
        df = pd.DataFrame(data)

    print(f"📊 Snapshot cargado: {len(df)} filas.")
    return df
