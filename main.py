"""
API de Conducta Prescriptiva — Sistema de Jobs Asíncronos
=========================================================
Endpoints:
  POST /analizar      → inicia job, devuelve job_id inmediatamente
  POST /exportar      → inicia job de exportación, devuelve job_id
  GET  /resultado/{job_id} → consulta estado/resultado del job
  GET  /health        → health check
"""

import os
import uuid
import asyncio
import logging
from datetime import datetime
from enum import Enum
from typing import Any
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
import io
import httpx

from scraper import descargar_ambos_excels, ParamsDescarga, ErrorLogin, ErrorDescarga
from motor import analizar
from exportador import generar_excel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="API Conducta Prescriptiva",
    description="Análisis automático de prescripciones vs consultas — GEA Sanatorios",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

API_KEY = os.getenv("API_KEY", "")

# ── Store de jobs en memoria ──────────────────────────────────────────────────

class EstadoJob(str, Enum):
    pendiente  = "pendiente"
    procesando = "procesando"
    completado = "completado"
    error      = "error"

jobs: dict[str, dict[str, Any]] = {}

def nuevo_job(tipo: str) -> str:
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "id":         job_id,
        "tipo":       tipo,
        "estado":     EstadoJob.pendiente,
        "creado":     datetime.utcnow().isoformat(),
        "actualizado": datetime.utcnow().isoformat(),
        "resultado":  None,
        "excel":      None,
        "error":      None,
    }
    return job_id

def actualizar_job(job_id: str, **kwargs):
    if job_id in jobs:
        jobs[job_id].update(kwargs)
        jobs[job_id]["actualizado"] = datetime.utcnow().isoformat()

# ── Seguridad ─────────────────────────────────────────────────────────────────

def verificar_api_key(x_api_key: str = Header(default="")):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida o ausente")

# ── Schema de request ─────────────────────────────────────────────────────────

class SolicitudAnalisis(BaseModel):
    usuario:     str = Field(..., description="Usuario del HIS")
    password:    str = Field(..., description="Contraseña del HIS")
    centro:      str = Field(default="", description="Centro de atención. Vacío = todos")
    fecha_desde: str = Field(..., description="DD/MM/AAAA — ej: 01/03/2026")
    fecha_hasta: str = Field(..., description="DD/MM/AAAA — ej: 31/03/2026")

    @validator("fecha_desde", "fecha_hasta")
    def validar_fecha(cls, v):
        import re
        if not re.match(r"^\d{2}/\d{2}/\d{4}$", v):
            raise ValueError("Formato de fecha inválido. Usar DD/MM/AAAA")
        return v

# ── Tarea de fondo ────────────────────────────────────────────────────────────

async def ejecutar_analisis(job_id: str, solicitud: SolicitudAnalisis, modo: str):
    actualizar_job(job_id, estado=EstadoJob.procesando)
    logger.info(f"[{job_id}] Iniciando análisis | centro={solicitud.centro or 'TODOS'} | {solicitud.fecha_desde} → {solicitud.fecha_hasta}")

    try:
        params = ParamsDescarga(
            usuario=solicitud.usuario,
            password=solicitud.password,
            centro=solicitud.centro,
            fecha_desde=solicitud.fecha_desde,
            fecha_hasta=solicitud.fecha_hasta,
        )

        raw_consultas, raw_prescripciones = await descargar_ambos_excels(params)
        logger.info(f"[{job_id}] Archivos descargados — procesando...")

        resultado = analizar(raw_consultas, raw_prescripciones)

        if modo == "exportar":
            excel_bytes = generar_excel(resultado)
            resultado.pop("_dataframes", None)
            actualizar_job(job_id,
                estado=EstadoJob.completado,
                resultado=resultado,
                excel=excel_bytes,
            )
        else:
            resultado.pop("_dataframes", None)
            actualizar_job(job_id,
                estado=EstadoJob.completado,
                resultado=resultado,
            )

        logger.info(f"[{job_id}] Completado")

    except ErrorLogin as e:
        logger.error(f"[{job_id}] Error de login: {e}")
        actualizar_job(job_id, estado=EstadoJob.error, error=f"Login fallido: {e}")
    except ErrorDescarga as e:
        logger.error(f"[{job_id}] Error de descarga: {e}")
        actualizar_job(job_id, estado=EstadoJob.error, error=f"Error al descargar datos: {e}")
    except Exception as e:
        logger.error(f"[{job_id}] Error inesperado: {e}")
        actualizar_job(job_id, estado=EstadoJob.error, error=f"Error inesperado: {e}")

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0", "jobs_activos": len(jobs)}


@app.post(
    "/analizar",
    summary="Iniciar análisis de conducta prescriptiva",
    description="Inicia el análisis en segundo plano y devuelve un job_id. Consultá el resultado con GET /resultado/{job_id}",
    dependencies=[Depends(verificar_api_key)],
)
async def analizar_endpoint(
    solicitud: SolicitudAnalisis,
    background_tasks: BackgroundTasks,
):
    job_id = nuevo_job("analizar")
    background_tasks.add_task(ejecutar_analisis, job_id, solicitud, "analizar")
    return {
        "job_id": job_id,
        "estado": EstadoJob.pendiente,
        "mensaje": "Análisis iniciado. Consultá el resultado en GET /resultado/{job_id}",
        "resultado_url": f"/resultado/{job_id}",
    }


@app.post(
    "/exportar",
    summary="Iniciar exportación como Excel",
    description="Igual que /analizar pero el resultado incluye un Excel descargable en GET /resultado/{job_id}/excel",
    dependencies=[Depends(verificar_api_key)],
)
async def exportar_endpoint(
    solicitud: SolicitudAnalisis,
    background_tasks: BackgroundTasks,
):
    job_id = nuevo_job("exportar")
    background_tasks.add_task(ejecutar_analisis, job_id, solicitud, "exportar")
    return {
        "job_id": job_id,
        "estado": EstadoJob.pendiente,
        "mensaje": "Exportación iniciada. Cuando el estado sea 'completado', descargá el Excel en GET /resultado/{job_id}/excel",
        "resultado_url": f"/resultado/{job_id}",
        "excel_url":     f"/resultado/{job_id}/excel",
    }


@app.get(
    "/resultado/{job_id}",
    summary="Consultar estado y resultado de un job",
    dependencies=[Depends(verificar_api_key)],
)
def resultado_endpoint(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' no encontrado")

    respuesta = {
        "id":         job["id"],
        "tipo":       job["tipo"],
        "estado":     job["estado"],
        "creado":     job["creado"],
        "actualizado": job["actualizado"],
    }

    if job["estado"] == EstadoJob.error:
        respuesta["error"] = job["error"]

    if job["estado"] == EstadoJob.completado:
        respuesta["resultado"] = job["resultado"]
        if job["excel"]:
            respuesta["excel_disponible"] = True
            respuesta["excel_url"] = f"/resultado/{job_id}/excel"

    return JSONResponse(content=respuesta)


@app.get(
    "/resultado/{job_id}/excel",
    summary="Descargar Excel de un job completado",
    dependencies=[Depends(verificar_api_key)],
)
def descargar_excel_endpoint(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' no encontrado")

    if job["estado"] != EstadoJob.completado:
        raise HTTPException(
            status_code=400,
            detail=f"El job está en estado '{job['estado']}' — esperá a que esté 'completado'"
        )

    if not job["excel"]:
        raise HTTPException(status_code=400, detail="Este job no generó un Excel (usá /exportar en vez de /analizar)")

    centro = (job["resultado"] or {}).get("metadata", {}).get("centros", [""])[0]
    centro_limpio = centro[:20].replace(" ", "_").replace("/", "-") if centro else "todos"
    nombre = f"Prescriptiva_{centro_limpio}_{job['creado'][:10]}.xlsx"

    return StreamingResponse(
        io.BytesIO(job["excel"]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{nombre}"'},
    )


@app.post(
    "/profesional",
    summary="Detalle de un profesional específico",
    dependencies=[Depends(verificar_api_key)],
)
async def profesional_endpoint(
    solicitud: SolicitudAnalisis,
    background_tasks: BackgroundTasks,
    nombre_profesional: str = "",
):
    job_id = nuevo_job("profesional")

    async def tarea_profesional(job_id, solicitud, nombre):
        await ejecutar_analisis(job_id, solicitud, "analizar")
        if jobs[job_id]["estado"] == EstadoJob.completado and nombre:
            nombre_upper = nombre.upper().strip()
            resultado = jobs[job_id]["resultado"]
            filtrado = {}
            for esc_key in ["escenario_1", "escenario_2", "escenario_3"]:
                esc = resultado.get(esc_key, {})
                filtrado[esc_key] = {
                    "nombre": esc.get("nombre"),
                    "por_profesional": [
                        r for r in esc.get("por_profesional", [])
                        if nombre_upper in str(r.get("personal", "")).upper()
                    ],
                }
            jobs[job_id]["resultado"] = filtrado

    background_tasks.add_task(tarea_profesional, job_id, solicitud, nombre_profesional)
    return {
        "job_id": job_id,
        "estado": EstadoJob.pendiente,
        "resultado_url": f"/resultado/{job_id}",
    }


# ── Proxy Claude ──────────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

class ProxyRequest(BaseModel):
    model: str = "claude-sonnet-4-20250514"
    max_tokens: int = 16000
    messages: list

@app.post(
    "/claude",
    summary="Proxy hacia la API de Anthropic",
    description="Reenvía el request a api.anthropic.com usando la API key configurada en Railway.",
)
async def proxy_claude(body: ProxyRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY no configurada en Railway")

    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": body.model,
                "max_tokens": body.max_tokens,
                "messages": body.messages,
            },
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return resp.json()
