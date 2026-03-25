"""
API de Conducta Prescriptiva
============================
Endpoints:
  POST /analizar    — devuelve JSON con los 3 escenarios
  POST /exportar    — devuelve el Excel descargable
  GET  /health      — health check para Railway

Seguridad: API key via header X-API-Key (configurar en variable de entorno API_KEY)
"""

import os
import logging
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field, validator
import io

from scraper import descargar_ambos_excels, ParamsDescarga, ErrorLogin, ErrorDescarga
from motor import analizar
from exportador import generar_excel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="API Conducta Prescriptiva",
    description="Análisis automático de prescripciones vs consultas — GEA Sanatorios",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

API_KEY = os.getenv("API_KEY", "")


def verificar_api_key(x_api_key: str = Header(default="")):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida o ausente")


class SolicitudAnalisis(BaseModel):
    usuario: str = Field(..., description="Usuario del HIS (hospital.sdlc.com.ar)")
    password: str = Field(..., description="Contraseña del HIS")
    centro: str = Field(default="", description="Centro de atención. Vacío = todos los centros")
    fecha_desde: str = Field(..., description="Fecha inicio en formato DD/MM/AAAA. Ej: 01/03/2026")
    fecha_hasta: str = Field(..., description="Fecha fin en formato DD/MM/AAAA. Ej: 31/03/2026")

    @validator("fecha_desde", "fecha_hasta")
    def validar_fecha(cls, v):
        import re
        if not re.match(r"^\d{2}/\d{2}/\d{4}$", v):
            raise ValueError("La fecha debe tener el formato DD/MM/AAAA. Ej: 01/03/2026")
        return v


async def _ejecutar_pipeline(solicitud: SolicitudAnalisis) -> dict:
    params = ParamsDescarga(
        usuario=solicitud.usuario,
        password=solicitud.password,
        centro=solicitud.centro,
        fecha_desde=solicitud.fecha_desde,
        fecha_hasta=solicitud.fecha_hasta,
    )

    logger.info(f"Iniciando análisis | centro={solicitud.centro or 'TODOS'} "
                f"| {solicitud.fecha_desde} → {solicitud.fecha_hasta}")

    try:
        raw_consultas, raw_prescripciones = await descargar_ambos_excels(params)
    except ErrorLogin as e:
        raise HTTPException(status_code=401, detail=str(e))
    except ErrorDescarga as e:
        raise HTTPException(status_code=502, detail=f"Error al descargar datos del HIS: {e}")
    except Exception as e:
        logger.error(f"Error inesperado en scraper: {e}")
        raise HTTPException(status_code=500, detail=f"Error inesperado: {e}")

    try:
        resultado = analizar(raw_consultas, raw_prescripciones)
    except Exception as e:
        logger.error(f"Error en motor de análisis: {e}")
        raise HTTPException(status_code=500, detail=f"Error al procesar los datos: {e}")

    return resultado


@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0"}


@app.post(
    "/analizar",
    summary="Analizar conducta prescriptiva",
    description=(
        "Conecta al HIS con las credenciales provistas, descarga los datos del período "
        "indicado y retorna el análisis completo en JSON con los 3 escenarios."
    ),
    dependencies=[Depends(verificar_api_key)],
)
async def analizar_endpoint(solicitud: SolicitudAnalisis):
    resultado = await _ejecutar_pipeline(solicitud)
    resultado.pop("_dataframes", None)
    return JSONResponse(content=resultado)


@app.post(
    "/exportar",
    summary="Exportar análisis como Excel",
    description=(
        "Igual que /analizar pero retorna un archivo Excel descargable con "
        "todas las hojas: Resumen, 3 escenarios, Sin prescripción y Desvíos."
    ),
    dependencies=[Depends(verificar_api_key)],
)
async def exportar_endpoint(solicitud: SolicitudAnalisis):
    resultado = await _ejecutar_pipeline(solicitud)

    try:
        excel_bytes = generar_excel(resultado)
    except Exception as e:
        logger.error(f"Error generando Excel: {e}")
        raise HTTPException(status_code=500, detail=f"Error al generar Excel: {e}")

    centro_limpio = (solicitud.centro or "todos").replace(" ", "_").replace("/", "-")[:20]
    desde = solicitud.fecha_desde.replace("/", "")
    hasta = solicitud.fecha_hasta.replace("/", "")
    nombre = f"Prescriptiva_{centro_limpio}_{desde}_{hasta}.xlsx"

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{nombre}"'},
    )


@app.post(
    "/profesional",
    summary="Detalle de un profesional específico",
    description="Retorna los 3 escenarios filtrados para un solo profesional.",
    dependencies=[Depends(verificar_api_key)],
)
async def profesional_endpoint(solicitud: SolicitudAnalisis, nombre_profesional: str):
    resultado = await _ejecutar_pipeline(solicitud)
    nombre_upper = nombre_profesional.upper().strip()

    detalle = {}
    for esc_key in ["escenario_1", "escenario_2", "escenario_3"]:
        esc = resultado.get(esc_key, {})
        detalle[esc_key] = {
            "nombre": esc.get("nombre"),
            "por_profesional": [
                r for r in esc.get("por_profesional", [])
                if nombre_upper in str(r.get("personal", "")).upper()
            ],
        }

    return JSONResponse(content=detalle)
