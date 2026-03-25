"""
Motor de análisis de conducta prescriptiva.
Recibe los dos Excel en bytes y retorna los resultados estructurados.
"""

import io
import pandas as pd
import numpy as np
from typing import Any
from clasificador import es_pami, es_laboratorio, es_consulta, clasificar_practica

COLS_CONSULTAS = {
    "personal": ["Personal", "PERSONAL"],
    "servicio": ["Servicio", "SERVICIO"],
    "centro":   ["Centro Atención", "Centro Atencion", "CENTRO ATENCION"],
    "convenio": ["Convenio", "CONVENIO"],
    "codigo":   ["Código Prestación", "Codigo Prestacion", "Código prestación"],
    "prestacion":["Prestación", "Prestacion", "PRESTACION"],
}

COLS_PRESCRIPCIONES = {
    "prescriptor": ["Prescriptor", "PRESCRIPTOR"],
    "servicio":    ["Servicio", "SERVICIO"],
    "obra_social": ["Obra Social", "Obra social", "OBRA SOCIAL"],
    "codigo":      ["Código", "Codigo", "CODIGO"],
    "prestacion":  ["Prestación", "Prestacion", "PRESTACION"],
    "fecha":       ["Fecha Emisión", "Fecha Emision", "FECHA EMISION"],
}


def _find_col(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    for c in candidatos:
        if c in df.columns:
            return c
    return None


def _leer_consultas(raw: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(raw))
    df.columns = df.columns.str.strip()

    col_map = {}
    for campo, candidatos in COLS_CONSULTAS.items():
        col = _find_col(df, candidatos)
        if col:
            col_map[col] = campo

    df = df.rename(columns=col_map)
    df = df[list(col_map.values())].copy()

    # Filtrar solo filas que sean consultas médicas
    if "codigo" in df.columns:
        df["es_consulta"] = df["codigo"].apply(es_consulta)
        df = df[df["es_consulta"]].copy()
    
    df["personal"]  = df.get("personal",  pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    df["servicio"]  = df.get("servicio",  pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    df["centro"]    = df.get("centro",    pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    df["convenio"]  = df.get("convenio",  pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    df["es_pami"]   = df["convenio"].apply(es_pami)

    return df


def _leer_prescripciones(raw: bytes) -> pd.DataFrame:
    # El archivo tiene metadatos en las primeras filas (Fecha Desde, Centro, etc.)
    # Buscar la fila que contiene "Fecha Emisión" como header real
    df_raw = pd.read_excel(io.BytesIO(raw), header=None)
    header_row = None
    for i, row in df_raw.iterrows():
        if any("fecha" in str(v).lower() for v in row.values):
            header_row = i
            break

    if header_row is None:
        header_row = 8  # fallback

    df = pd.read_excel(io.BytesIO(raw), header=header_row)
    df.columns = df.columns.str.strip()

    # Extraer centro del encabezado si está disponible
    centro_header = None
    for i in range(min(header_row, df_raw.shape[0])):
        row = df_raw.iloc[i]
        for j, val in enumerate(row.values):
            if "centro" in str(val).lower() and j + 1 < len(row.values):
                centro_header = str(row.values[j + 1]).strip().upper()
                break

    col_map = {}
    for campo, candidatos in COLS_PRESCRIPCIONES.items():
        col = _find_col(df, candidatos)
        if col:
            col_map[col] = campo

    df = df.rename(columns=col_map)
    cols_presentes = [c for c in col_map.values() if c in df.columns]
    df = df[cols_presentes].dropna(subset=["prescriptor"] if "prescriptor" in cols_presentes else cols_presentes[:1]).copy()

    df["prescriptor"] = df.get("prescriptor", pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    df["servicio"]    = df.get("servicio",    pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    df["obra_social"] = df.get("obra_social", pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    df["prestacion"]  = df.get("prestacion",  pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    df["codigo"]      = df.get("codigo",      pd.Series(dtype=str)).astype(str).str.strip()

    df["es_pami"]       = df["obra_social"].apply(es_pami)
    df["es_lab"]        = df["prestacion"].apply(es_laboratorio)
    df["tipo_practica"] = df["prestacion"].apply(clasificar_practica)
    df["centro"]        = centro_header or "NO ESPECIFICADO"

    return df


def _agregar_consultas(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["personal", "servicio", "centro", "convenio", "es_pami"])
        .size()
        .reset_index(name="n_consultas")
    )


def _agregar_prescripciones(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["prescriptor", "servicio", "centro", "obra_social",
                    "es_pami", "es_lab", "tipo_practica"])
        .size()
        .reset_index(name="n_prescripciones")
    )


def _cruzar(cons_agg: pd.DataFrame, presc_agg: pd.DataFrame,
            filtrar_pami: bool, filtrar_lab: bool) -> pd.DataFrame:

    cons = cons_agg.copy()
    presc = presc_agg.copy()

    if filtrar_pami:
        cons  = cons[~cons["es_pami"]].copy()
        presc = presc[~presc["es_pami"]].copy()

    if filtrar_lab:
        presc = presc[~presc["es_lab"]].copy()

    # Sumar consultas por profesional × servicio × centro
    den = (
        cons.groupby(["personal", "servicio", "centro"])["n_consultas"]
        .sum()
        .reset_index()
    )

    # Sumar prescripciones por prescriptor × servicio × centro
    num = (
        presc.groupby(["prescriptor", "servicio", "centro"])["n_prescripciones"]
        .sum()
        .reset_index()
        .rename(columns={"prescriptor": "personal"})
    )

    # Left join desde denominador — mantiene profesionales sin prescripciones
    resultado = den.merge(num, on=["personal", "servicio", "centro"], how="left")
    resultado["n_prescripciones"] = resultado["n_prescripciones"].fillna(0).astype(int)
    resultado["ratio"] = np.where(
        resultado["n_consultas"] > 0,
        resultado["n_prescripciones"] / resultado["n_consultas"],
        0.0
    )
    resultado["ratio"] = resultado["ratio"].round(2)
    resultado["sin_prescripcion"] = resultado["n_prescripciones"] == 0

    return resultado.sort_values("ratio", ascending=False).reset_index(drop=True)


def _desvios(df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    media  = df["ratio"].mean()
    desvio = df["ratio"].std()
    df = df.copy()
    df["z_score"] = ((df["ratio"] - media) / desvio).round(2) if desvio > 0 else 0.0
    df["alerta"] = pd.cut(
        df["ratio"],
        bins=[-np.inf, media + desvio, media + 2 * desvio, np.inf],
        labels=["normal", "atención", "crítico"]
    )
    return df.nlargest(top_n, "ratio")


def analizar(raw_consultas: bytes, raw_prescripciones: bytes) -> dict[str, Any]:
    """
    Punto de entrada principal.
    Retorna un dict con todos los resultados listos para serializar.
    """
    df_cons  = _leer_consultas(raw_consultas)
    df_presc = _leer_prescripciones(raw_prescripciones)

    cons_agg  = _agregar_consultas(df_cons)
    presc_agg = _agregar_prescripciones(df_presc)

    # Los tres escenarios
    esc1 = _cruzar(cons_agg, presc_agg, filtrar_pami=False, filtrar_lab=False)
    esc2 = _cruzar(cons_agg, presc_agg, filtrar_pami=True,  filtrar_lab=False)
    esc3 = _cruzar(cons_agg, presc_agg, filtrar_pami=True,  filtrar_lab=True)

    return {
        "metadata": {
            "total_consultas_bruto": len(df_cons),
            "total_prescripciones_bruto": len(df_presc),
            "profesionales_unicos": df_cons["personal"].nunique(),
            "centros": sorted(df_cons["centro"].unique().tolist()),
            "servicios": sorted(df_cons["servicio"].unique().tolist()),
        },
        "escenario_1": _resumir(esc1, "Completo"),
        "escenario_2": _resumir(esc2, "Sin PAMI"),
        "escenario_3": _resumir(esc3, "Sin PAMI ni laboratorio"),
        "_dataframes": {"esc1": esc1, "esc2": esc2, "esc3": esc3,
                        "df_presc": df_presc, "df_cons": df_cons},
    }


def _resumir(df: pd.DataFrame, nombre: str) -> dict:
    total_consultas = int(df["n_consultas"].sum())
    total_presc     = int(df["n_prescripciones"].sum())
    con_presc       = int((df["n_prescripciones"] > 0).sum())
    sin_presc       = int((df["n_prescripciones"] == 0).sum())
    ratio_global    = round(total_presc / total_consultas, 2) if total_consultas > 0 else 0

    return {
        "nombre": nombre,
        "total_consultas": total_consultas,
        "total_prescripciones": total_presc,
        "profesionales_con_prescripcion": con_presc,
        "profesionales_sin_prescripcion": sin_presc,
        "ratio_global": ratio_global,
        "por_profesional": _df_to_records(df),
        "por_servicio": _df_to_records(
            df.groupby("servicio").agg(
                n_consultas=("n_consultas", "sum"),
                n_prescripciones=("n_prescripciones", "sum")
            ).assign(ratio=lambda x: (x["n_prescripciones"] / x["n_consultas"]).round(2))
            .reset_index().sort_values("ratio", ascending=False)
        ),
        "por_centro": _df_to_records(
            df.groupby("centro").agg(
                n_consultas=("n_consultas", "sum"),
                n_prescripciones=("n_prescripciones", "sum")
            ).assign(ratio=lambda x: (x["n_prescripciones"] / x["n_consultas"]).round(2))
            .reset_index().sort_values("ratio", ascending=False)
        ),
        "top_desvios": _df_to_records(_desvios(df)),
        "sin_prescripcion": _df_to_records(df[df["sin_prescripcion"]]),
    }


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    return df.replace({np.nan: None}).to_dict(orient="records")
