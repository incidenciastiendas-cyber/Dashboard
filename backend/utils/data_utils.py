import gzip
import json
import pandas as pd

def cargar_snapshot_json_gz(path_gz):
    with gzip.open(path_gz, 'rt', encoding='utf-8') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def enriquecer_incidencias(df_incidencias, path_tiendas, path_distritos):
    df_tiendas = pd.read_csv(path_tiendas, dtype={"Tienda": str})
    df_distritos = pd.read_csv(path_distritos, dtype={"Tienda": str})
    df_incidencias["N° Tienda"] = df_incidencias["N° Tienda"].astype(str)
    df = df_incidencias.merge(df_tiendas, how="left", left_on="N° Tienda", right_on="Tienda")
    df = df.merge(df_distritos, how="left", on="Tienda")
    cols_extra = ["Nombre", "Provincia", "Direccion", "Distrito", "Distrital"]
    return df[[*df_incidencias.columns, *cols_extra]]
