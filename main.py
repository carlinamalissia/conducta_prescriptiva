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


# ── Proxy Anthropic ───────────────────────────────────────────────────────────

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

    # Normalizar messages: Anthropic acepta content como string o lista
    messages = []
    for msg in body.messages:
        if isinstance(msg, dict):
            role = msg.get("role", "user")
            content = msg.get("content", "")
            # Si content es lista, convertir a string
            if isinstance(content, list):
                content = " ".join(
                    block.get("text", "") if isinstance(block, dict) else str(block)
                    for block in content
                )
            messages.append({"role": role, "content": str(content)})
        else:
            messages.append({"role": "user", "content": str(msg)})

    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": body.max_tokens,
                "messages": messages,
            },
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return resp.json()


# ── Servir la app web ─────────────────────────────────────────────────────────

from fastapi.responses import HTMLResponse

HTML_APP = '<!DOCTYPE html>\n<html lang="es">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width, initial-scale=1.0">\n<title>Análisis de Prescripciones — Sanatorio de la Cañada</title>\n<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>\n<style>\n  @import url(\'https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Sora:wght@300;400;600;700&display=swap\');\n  :root {\n    --teal:#0F6E56; --teal-mid:#1D9E75; --teal-light:#E1F5EE;\n    --amber:#BA7517; --amber-bg:#FAEEDA;\n    --red:#A32D2D; --red-bg:#FCEBEB;\n    --border:#DDD; --gray-50:#FAFAF8; --gray-100:#F4F3EF;\n    --gray-400:#888; --gray-700:#3D3D3A; --gray-900:#1A1A18;\n  }\n  *{box-sizing:border-box;margin:0;padding:0}\n  body{font-family:\'Sora\',sans-serif;background:var(--gray-50);color:var(--gray-700);min-height:100vh}\n  header{background:var(--teal);padding:18px 40px;display:flex;align-items:center;gap:14px}\n  .logo{width:36px;height:36px;background:rgba(255,255,255,.18);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:18px}\n  header h1{font-size:17px;font-weight:600;color:#fff}\n  header p{font-size:12px;color:rgba(255,255,255,.55);font-weight:300}\n  .badge{margin-left:auto;background:rgba(255,255,255,.15);color:#fff;font-size:11px;font-family:\'DM Mono\',monospace;padding:4px 12px;border-radius:20px;white-space:nowrap}\n  main{max-width:720px;margin:44px auto;padding:0 20px}\n  .lbl{font-size:10.5px;font-family:\'DM Mono\',monospace;letter-spacing:1.6px;text-transform:uppercase;color:var(--teal);margin-bottom:14px}\n  .api-wrap{position:relative;margin-bottom:36px}\n  .api-wrap input{width:100%;padding:13px 16px;border:1.5px solid var(--border);border-radius:9px;font-family:\'DM Mono\',monospace;font-size:13px;background:white;outline:none;transition:border-color .2s}\n  .api-wrap input:focus{border-color:var(--teal)}\n  .api-hint{font-size:11px;color:var(--gray-400);margin-top:6px}\n  .upload-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:24px}\n  .dz{position:relative;border:2px dashed var(--border);border-radius:12px;padding:26px 16px 22px;text-align:center;cursor:pointer;transition:all .2s;background:white}\n  .dz:hover,.dz.over{border-color:var(--teal-mid);background:var(--teal-light)}\n  .dz.done{border:2px solid var(--teal);background:var(--teal-light)}\n  .dz input{position:absolute;inset:0;opacity:0;cursor:pointer}\n  .dz-icon{width:42px;height:42px;border-radius:10px;background:var(--gray-100);margin:0 auto 10px;display:flex;align-items:center;justify-content:center;font-size:22px;transition:background .2s}\n  .dz.done .dz-icon{background:var(--teal)}\n  .dz-title{font-size:13px;font-weight:600;color:var(--gray-900);margin-bottom:3px}\n  .dz-sub{font-size:11px;color:var(--gray-400)}\n  .dz-name{font-size:11px;font-family:\'DM Mono\',monospace;color:var(--teal);margin-top:7px;word-break:break-all}\n  .period-row{display:flex;align-items:center;gap:12px;margin-bottom:28px}\n  .period-row label{font-size:12px;color:var(--gray-400);white-space:nowrap}\n  .period-row input[type=month]{padding:10px 14px;border:1.5px solid var(--border);border-radius:8px;font-family:\'Sora\',sans-serif;font-size:13px;background:white;outline:none;transition:border-color .2s}\n  .period-row input[type=month]:focus{border-color:var(--teal)}\n  .btn{width:100%;padding:15px;background:var(--teal);color:white;border:none;border-radius:10px;font-family:\'Sora\',sans-serif;font-size:15px;font-weight:600;cursor:pointer;letter-spacing:-.2px;transition:all .2s}\n  .btn:hover:not(:disabled){background:#0a5a45;transform:translateY(-1px)}\n  .btn:disabled{opacity:.45;cursor:not-allowed;transform:none}\n  .status{margin-top:24px;border-radius:12px;padding:18px 22px;display:none}\n  .status.show{display:block}\n  .status.processing{background:var(--amber-bg);border:1px solid #e8c06a}\n  .status.success{background:var(--teal-light);border:1px solid #9fe1cb}\n  .status.error{background:var(--red-bg);border:1px solid #f7c1c1}\n  .st-head{display:flex;align-items:center;gap:9px;margin-bottom:8px}\n  .st-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}\n  .processing .st-dot{background:var(--amber);animation:blink 1.1s infinite}\n  .success .st-dot{background:var(--teal)}\n  .error .st-dot{background:var(--red)}\n  @keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}\n  .st-title{font-size:13px;font-weight:600;color:var(--gray-900)}\n  .st-log{font-size:11.5px;font-family:\'DM Mono\',monospace;color:var(--gray-700);line-height:1.8;max-height:160px;overflow-y:auto}\n  .btn-dl{display:inline-flex;align-items:center;gap:8px;margin-top:14px;padding:11px 20px;background:var(--teal);color:white;border:none;border-radius:8px;font-family:\'Sora\',sans-serif;font-size:13px;font-weight:600;cursor:pointer;text-decoration:none;transition:background .2s}\n  .btn-dl:hover{background:#0a5a45}\n  hr{border:none;border-top:1px solid var(--border);margin:36px 0}\n  .card{background:white;border:1px solid var(--border);border-radius:12px;padding:18px 22px}\n  .card h4{font-size:12px;font-weight:600;margin-bottom:10px}\n  .card ul{list-style:none;display:flex;flex-direction:column;gap:5px}\n  .card li{font-size:11.5px;color:var(--gray-400);padding-left:16px;position:relative}\n  .card li::before{content:\'→\';position:absolute;left:0;color:var(--teal-mid)}\n  .esc-tags{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px}\n  .tag{font-size:10.5px;font-family:\'DM Mono\',monospace;padding:4px 10px;border-radius:4px;font-weight:500}\n  .t1{background:#dbeafe;color:#1e40af}.t2{background:#ccfbf1;color:#0f766e}.t3{background:#dcfce7;color:#166534}\n</style>\n</head>\n<body>\n<header>\n  <div class="logo">💊</div>\n  <div><h1>Análisis de Conducta Prescriptiva</h1><p>SANATORIO DE LA CAÑADA — Atenciones Ambulatorias</p></div>\n  <span class="badge">v4 · 28 categorías</span>\n</header>\n<main>\n  <p class="lbl">Archivos del período</p>\n  <div class="upload-grid">\n    <div class="dz" id="z1" ondragover="ev(event,\'z1\')" ondragleave="lv(\'z1\')" ondrop="dp(event,\'z1\',\'f1\')">\n      <input type="file" id="f1" accept=".xlsx,.xls" onchange="loaded(\'f1\',\'z1\',\'i1\',\'n1\')">\n      <div class="dz-icon" id="i1">📋</div>\n      <p class="dz-title">Atenciones</p><p class="dz-sub">consultaAtenciones.xlsx</p>\n      <p class="dz-name" id="n1"></p>\n    </div>\n    <div class="dz" id="z2" ondragover="ev(event,\'z2\')" ondragleave="lv(\'z2\')" ondrop="dp(event,\'z2\',\'f2\')">\n      <input type="file" id="f2" accept=".xlsx,.xls" onchange="loaded(\'f2\',\'z2\',\'i2\',\'n2\')">\n      <div class="dz-icon" id="i2">💊</div>\n      <p class="dz-title">Prescripciones</p><p class="dz-sub">consultaPrescripciones.xlsx</p>\n      <p class="dz-name" id="n2"></p>\n    </div>\n  </div>\n  <div class="period-row">\n    <label>Período:</label>\n    <input type="month" id="periodo">\n  </div>\n  <button class="btn" id="btnRun" onclick="run()">Generar análisis</button>\n  <div class="status" id="stBox">\n    <div class="st-head"><div class="st-dot"></div><span class="st-title" id="stTitle"></span></div>\n    <div class="st-log" id="stLog"></div>\n    <div id="dlArea"></div>\n  </div>\n  <hr>\n  <div class="card">\n    <h4>Metodología aplicada</h4>\n    <ul>\n      <li>Denominador: todas las consultas (episodios sin prescripción → n_presc=0)</li>\n      <li>Join: Paciente + Profesional + Fecha</li>\n      <li>Ratio = Total Prescripciones / Total Consultas</li>\n      <li>% Con Presc = episodios con ≥1 prescripción / total consultas</li>\n      <li>Excluye vacuna antigripal y prestaciones de vacunación</li>\n      <li>PAMI incluye PAMI SDLC y PAMI OFTAL</li>\n      <li>28 categorías de práctica · orden de prioridad definido</li>\n      <li>Semáforos 🟢🟡🔴 en Ratio y % Con Presc</li>\n    </ul>\n    <div class="esc-tags">\n      <span class="tag t1">Esc 1 — Completo</span>\n      <span class="tag t2">Esc 2 — Sin PAMI</span>\n      <span class="tag t3">Esc 3 — Sin PAMI ni Lab</span>\n    </div>\n  </div>\n</main>\n<script>\nlet wb1=null,wb2=null;\nconst hoy=new Date();\ndocument.getElementById(\'periodo\').value=`${hoy.getFullYear()}-${String(hoy.getMonth()+1).padStart(2,\'0\')}`;\nfunction g(id){return document.getElementById(id)}\nfunction ev(e,z){e.preventDefault();g(z).classList.add(\'over\')}\nfunction lv(z){g(z).classList.remove(\'over\')}\nfunction dp(e,z,fid){e.preventDefault();lv(z);const f=e.dataTransfer.files[0];if(!f)return;const dt=new DataTransfer();dt.items.add(f);g(fid).files=dt.files;g(fid).dispatchEvent(new Event(\'change\'))}\nfunction loaded(fid,zid,iid,nid){const file=g(fid).files[0];if(!file)return;g(zid).classList.remove(\'over\');g(zid).classList.add(\'done\');g(iid).textContent=\'✓\';g(nid).textContent=file.name;const reader=new FileReader();reader.onload=e=>{const wb=XLSX.read(e.target.result,{type:\'array\',cellDates:true});if(fid===\'f1\')wb1=wb;else wb2=wb};reader.readAsArrayBuffer(file)}\nfunction setStatus(tipo,titulo){const box=g(\'stBox\');box.className=\'status show \'+tipo;g(\'stTitle\').textContent=titulo;g(\'stLog\').textContent=\'\';g(\'dlArea\').innerHTML=\'\'}\nfunction log(msg){const el=g(\'stLog\');el.textContent+=(el.textContent?\'\\n\':\'\')+msg;el.scrollTop=el.scrollHeight}\nfunction toCSV(wb,idx=0){const sheet=wb.Sheets[wb.SheetNames[idx]];return XLSX.utils.sheet_to_csv(sheet,{blankrows:false})}\nfunction limitCSV(csv,max=90000){return csv.length>max?csv.slice(0,max)+\'\\n[...datos truncados]\':csv}\nfunction detectCentro(wb){try{const sheet=wb.Sheets[wb.SheetNames[0]];const rows=XLSX.utils.sheet_to_json(sheet,{header:1,defval:\'\'});for(let i=0;i<Math.min(8,rows.length);i++){for(let j=0;j<rows[i].length;j++){if(String(rows[i][j]).toLowerCase().includes(\'centro\')&&j+1<rows[i].length){const v=String(rows[i][j+1]).trim();if(v&&v.length>2)return v}}}}catch(e){}return\'Todos los centros\'}\n\nconst RAILWAY_URL = \'https://conductaprescriptiva-production.up.railway.app/claude\';\n\nasync function run(){\n  const periodo=g(\'periodo\').value;\n  if(!wb1)return alert(\'Cargá el archivo de Atenciones\');\n  if(!wb2)return alert(\'Cargá el archivo de Prescripciones\');\n  g(\'btnRun\').disabled=true;\n  setStatus(\'processing\',\'Preparando datos...\');\n  try{\n    const csvAt=limitCSV(toCSV(wb1));\n    const csvPr=limitCSV(toCSV(wb2));\n    const centro=detectCentro(wb2);\n    const[yr,mo]=periodo.split(\'-\');\n    const perLabel=`${mo}/${yr}`;\n    log(`Centro: ${centro} | Período: ${perLabel}`);\n    setStatus(\'processing\',`Procesando con Claude — ${perLabel}`);\n    log(`Centro: ${centro}`);log(\'Esto puede tardar 1-2 minutos...\');\n    const res=await fetch(RAILWAY_URL,{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({model:\'claude-sonnet-4-20250514\',max_tokens:16000,messages:[{role:\'user\',content:buildPrompt(csvAt,csvPr,perLabel,centro)}]})});\n    if(!res.ok){const err=await res.json();throw new Error(err.error?.message||`HTTP ${res.status}`)}\n    const data=await res.json();\n    const texto=data.content.filter(b=>b.type===\'text\').map(b=>b.text).join(\'\');\n    log(\'Respuesta recibida. Parseando...\');\n    const m=texto.match(/```json\\s*([\\s\\S]*?)\\s*```/);\n    let resultado;\n    try{resultado=JSON.parse(m?m[1]:texto.trim())}catch(e){console.error(\'Respuesta Claude:\',texto);throw new Error(\'No se pudo parsear el JSON. Ver consola del browser.\')}\n    log(\'Generando Excel con 7 pestañas...\');\n    const xlsxBytes=buildExcel(resultado,perLabel,centro);\n    setStatus(\'success\',`✓ Análisis completado — ${perLabel}`);\n    log(`Centro: ${centro}`);\n    log(`Profesionales: ${(resultado.por_profesional||[]).length}`);\n    log(`Consultas totales (Esc 1): ${resultado.escenario_1?.total_consultas||\'—\'}`);\n    log(`Ratio global (Esc 3): ${resultado.escenario_3?.ratio?.toFixed(2)||\'—\'}`);\n    const blob=new Blob([xlsxBytes],{type:\'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\'});\n    const url=URL.createObjectURL(blob);\n    const fn=`Analisis_Prescripciones_${yr}-${mo}.xlsx`;\n    g(\'dlArea\').innerHTML=`<a class="btn-dl" href="${url}" download="${fn}">↓ Descargar Excel</a>`;\n  }catch(e){setStatus(\'error\',\'Error en el análisis\');log(e.message);console.error(e)}\n  finally{g(\'btnRun\').disabled=false}\n}\n\nfunction buildPrompt(csvAt,csvPr,periodo,centro){\nreturn `Sos un analista de datos de salud especializado en conducta prescriptiva de instituciones privadas de Argentina.\n\nAdjunto dos archivos CSV:\n- ARCHIVO 1 — ATENCIONES (denominador)\n- ARCHIVO 2 — PRESCRIPCIONES (numerador)\n\nPERÍODO: ${periodo}\nCENTRO: ${centro}\n\nARCHIVO 1 — ATENCIONES:\n\\`\\`\\`\n${csvAt}\n\\`\\`\\`\n\nARCHIVO 2 — PRESCRIPCIONES:\n\\`\\`\\`\n${csvPr}\n\\`\\`\\`\n\nMETODOLOGÍA EXACTA:\n- Denominador: TODAS las consultas del archivo de atenciones. Si un paciente tuvo consulta con un profesional ese día pero sin prescripción, cuenta como episodio con n_presc=0.\n- Join: Paciente + Profesional + Fecha.\n- Ratio = Total Prescripciones / Total Consultas (incluye consultas con 0 en el denominador).\n- % Con Presc = consultas que generaron al menos 1 prescripción / total consultas.\n- Excluir del análisis: vacuna antigripal y cualquier prestación de vacunación.\n- PAMI incluye PAMI SDLC y PAMI OFTAL, tanto en prescripciones como en atenciones.\n- Códigos de consulta válidos: 429926, 420101, 429965, 429921, 420405, 429957, 429950, 429901, 420701, 429949\n\nESCENARIOS:\n1. Completo — todos los datos\n2. Sin PAMI — excluir atenciones y prescripciones de PAMI\n3. Sin PAMI ni Laboratorio — ídem + excluir todas las prestaciones clasificadas como Laboratorio\n\nCLASIFICACIÓN (aplicar la primera regla que coincida):\n1. Ecografia ginecologica — ecografías con: tocogine, vaginal, mamari, obstet, pelvi, genito, endometri, uterina, utero, ovari (solo si no es ecodoppler ni ecocardiograma)\n2. Ecografia — resto de ecografías (excluir ecodoppler y ecocardiograma)\n3. Ecocardiograma — ecocardio, eco cardio\n4. Ecodoppler — ecodoppler, doppler\n5. TAC — tomografía, angiotomografía, pielotac. EXCLUIR: tacos histológicos→Biopsia; OCT→Oftalmología; eritrosedimentación,parathormona→Lab; fondo de ojo→Oftalmología\n6. RNM — resonancia, angioresonancia, colangioresonancia\n7. Radiografia — rx, radiografía, espinografía\n8. Mamografia — mamografía, senografía\n9. Densitometria — densitometría ósea\n10. Medicina nuclear — centellografía, gammagrafía, SPECT, PET\n11. Holter/MAPA/Ergometria — holter, MAPA, presurometría, ergometría\n12. ECG — electrocardiograma, telemetría. EXCLUIR: EEG→Estudio neurológico\n13. Espirometria — espirometría, broncoespirometría\n14. Estudio del sueno — polisomnografía, poligrafía respiratoria\n15. Estudio oftalmologico — campimetría, campo visual, OCT, fondo de ojo, retinografía, oftalmoscopía, topografía corneal, paquimetría, ecometría, recuento endotelio corneal\n16. Estudio neurologico — electroencefalograma/EEG, electromiografía, velocidad de conducción, potencial evocado, videonistagmografía\n17. Audiologia — audiometría, logoaudiometría, impedanciometría, otoemisiones, otomicroscopía, fonoaudiología (sesiones), rehabilitación del lenguaje, timpanometría\n18. Endoscopia — endoscopía, colonoscopía, gastroscopía, videocolon, videoesofago, fibroscopía, cistouretrofibroscopía, uretrocistoscopía, polipectomía endoscópica, elastografía fibroscan\n19. Estudio ginecologico (no eco) — colposcopía, papanicolau, histeroscopía, histerosalpingografía, vulvoscopía, citología oncológica. EXCLUIR: citológico completo→Lab; citología exfoliativa de líquidos/orinas→Lab\n20. Practica quirurgica — cirugías mayores y menores, procedimientos invasivos, anestesiología, colocación DIU/implante, suturas, escisión lesiones piel, RTU, nefrolitotomía, coronariografía, etc.\n21. Biopsia/Anat.Patologica — biopsia, medulograma, mielograma, revisión tacos histológicos, receptores hormonales inmunohistoquímica\n22. Kinesiologia/Rehabilitacion — fisioterapia, kinesioterapia, agentes físicos, hidroterapia, parafina, crioterapia, electroterapia, ultrasonido, tracción cervical\n23. Salud mental — psicología (sesiones), psicoterapia, psiquiatría, psicopedagogía\n24. Procedimiento terapeutico — transfusiones, infusión IV, venoclisis, quimioterapia\n25. Estudio funcional — estudio urodinámico, uroflujometría, test de caminata, FENO, dinámica del tránsito esofágico\n26. Estudio urologico/reproduc. — espermograma\n27. Consulta/Interconsulta — consulta en consultorio, consulta en guardia, interconsulta de cualquier especialidad, consulta fonoaudiología, consulta psicología primera vez o evaluación diagnóstica\n28. Laboratorio — todo lo restante: bioquímica, hemograma, coagulograma, hormonas, marcadores tumorales, serología, microbiología, inmunología, orina completa, gases, troponina, procalcitonina, citológico completo, citología exfoliativa, cultivos, bacteriología, etc.\n\nRESPONDE ÚNICAMENTE con JSON válido envuelto en \\`\\`\\`json, sin texto adicional:\n\n\\`\\`\\`json\n{\n  "periodo": "${periodo}",\n  "centro": "${centro}",\n  "escenario_1": {"nombre":"Completo","total_consultas":0,"con_prescripcion":0,"sin_prescripcion":0,"pct_con_presc":0.0,"total_prescripciones":0,"ratio":0.0},\n  "escenario_2": {"nombre":"Sin PAMI","total_consultas":0,"con_prescripcion":0,"sin_prescripcion":0,"pct_con_presc":0.0,"total_prescripciones":0,"ratio":0.0},\n  "escenario_3": {"nombre":"Sin PAMI ni Laboratorio","total_consultas":0,"con_prescripcion":0,"sin_prescripcion":0,"pct_con_presc":0.0,"total_prescripciones":0,"ratio":0.0},\n  "por_servicio": [{"servicio":"","esc1_consultas":0,"esc1_con_presc":0,"esc1_sin_presc":0,"esc1_pct":0.0,"esc1_total_presc":0,"esc1_ratio":0.0,"esc2_consultas":0,"esc2_con_presc":0,"esc2_sin_presc":0,"esc2_pct":0.0,"esc2_total_presc":0,"esc2_ratio":0.0,"esc3_consultas":0,"esc3_con_presc":0,"esc3_sin_presc":0,"esc3_pct":0.0,"esc3_total_presc":0,"esc3_ratio":0.0}],\n  "por_profesional": [{"profesional":"","servicio":"","esc1_consultas":0,"esc1_con_presc":0,"esc1_sin_presc":0,"esc1_pct":0.0,"esc1_total_presc":0,"esc1_ratio":0.0,"esc2_consultas":0,"esc2_con_presc":0,"esc2_sin_presc":0,"esc2_pct":0.0,"esc2_total_presc":0,"esc2_ratio":0.0,"esc3_consultas":0,"esc3_con_presc":0,"esc3_sin_presc":0,"esc3_pct":0.0,"esc3_total_presc":0,"esc3_ratio":0.0}],\n  "tipos_por_servicio_esc3": [{"servicio":"","tipo":"","consultas_episodio":0,"prescripciones":0,"ratio":0.0}],\n  "tipos_por_profesional_esc3": [{"profesional":"","servicio":"","tipo":"","consultas_episodio":0,"prescripciones":0,"ratio":0.0,"es_resumen":false,"sin_presc_pct":0.0}],\n  "duracion_vs_prescripcion": [\n    {"rango":"< 10 min","consultas":0,"con_presc":0,"sin_presc":0,"pct_con_presc":0.0,"ratio":0.0,"duracion_prom":0.0},\n    {"rango":"10-20 min","consultas":0,"con_presc":0,"sin_presc":0,"pct_con_presc":0.0,"ratio":0.0,"duracion_prom":0.0},\n    {"rango":"20-30 min","consultas":0,"con_presc":0,"sin_presc":0,"pct_con_presc":0.0,"ratio":0.0,"duracion_prom":0.0},\n    {"rango":"30-45 min","consultas":0,"con_presc":0,"sin_presc":0,"pct_con_presc":0.0,"ratio":0.0,"duracion_prom":0.0},\n    {"rango":"45-60 min","consultas":0,"con_presc":0,"sin_presc":0,"pct_con_presc":0.0,"ratio":0.0,"duracion_prom":0.0},\n    {"rango":"> 60 min","consultas":0,"con_presc":0,"sin_presc":0,"pct_con_presc":0.0,"ratio":0.0,"duracion_prom":0.0}\n  ],\n  "desvios_top12": [{"profesional":"","servicio":"","esc3_ratio":0.0,"esc3_consultas":0,"esc3_total_presc":0}],\n  "servicios_sin_prescripcion_esc3": [],\n  "notas": ""\n}\n\\`\\`\\``\n}\n\nfunction buildExcel(data,periodo,centro){\n  const wb=XLSX.utils.book_new();\n  const d=v=>(typeof v===\'number\'?v:null)??\'—\';\n  const semR=v=>{if(typeof v!==\'number\')return\'—\';const s=v.toFixed(2);if(v>=4)return\'🔴 \'+s;if(v>=2.5)return\'🟡 \'+s;return\'🟢 \'+s};\n  const semP=v=>{if(typeof v!==\'number\')return\'—\';const s=(v*100).toFixed(1)+\'%\';if(v>=0.60)return\'🔴 \'+s;if(v>=0.35)return\'🟡 \'+s;return\'🟢 \'+s};\n  const semPS=v=>{if(typeof v!==\'number\')return\'—\';const s=(v*100).toFixed(1)+\'%\';if(v>=0.75)return\'🔴 \'+s;if(v>=0.25)return\'🟡 \'+s;return\'🟢 \'+s};\n\n  const e1=data.escenario_1||{},e2=data.escenario_2||{},e3=data.escenario_3||{};\n\n  // Hoja 1\n  const h1=[\n    [`ANÁLISIS DE PRESCRIPCIONES POR CONSULTA — ${periodo}`],\n    [`Centro: ${centro} | 28 categorías | PAMI SDLC + PAMI OFTAL`],\n    [],[\'RESUMEN COMPARATIVO POR ESCENARIO\'],\n    [\'Escenario\',\'Consultas\',\'Con Presc\',\'Sin Presc\',\'% Con Presc\',\'Total Presc\',\'Ratio P/C\'],\n    [\'ESC 1 — COMPLETO\',d(e1.total_consultas),d(e1.con_prescripcion),d(e1.sin_prescripcion),semP((e1.pct_con_presc||0)/100),d(e1.total_prescripciones),semR(e1.ratio)],\n    [\'ESC 2 — SIN PAMI\',d(e2.total_consultas),d(e2.con_prescripcion),d(e2.sin_prescripcion),semP((e2.pct_con_presc||0)/100),d(e2.total_prescripciones),semR(e2.ratio)],\n    [\'ESC 3 — SIN PAMI NI LAB\',d(e3.total_consultas),d(e3.con_prescripcion),d(e3.sin_prescripcion),semP((e3.pct_con_presc||0)/100),d(e3.total_prescripciones),semR(e3.ratio)],\n    [],[\'NOTAS METODOLÓGICAS\'],\n    [\'• Denominador: todas las consultas (n_presc=0 si sin prescripción)\'],\n    [\'• Join: Paciente + Profesional + Fecha\'],\n    [\'• Ratio = Total Prescripciones / Total Consultas\'],\n    [\'• PAMI: incluye PAMI SDLC y PAMI OFTAL\'],\n    [\'• Excluye vacuna antigripal y prestaciones de vacunación\'],\n    [\'• Semáforo Ratio: 🟢<2.5 | 🟡2.5-4 | 🔴≥4\'],\n    [\'• Semáforo %ConPresc: 🟢<35% | 🟡35-60% | 🔴≥60%\'],\n    [],[data.notas||\'\'],\n  ];\n  const ws1=XLSX.utils.aoa_to_sheet(h1);\n  ws1[\'!cols\']=[{wch:36},{wch:13},{wch:13},{wch:13},{wch:16},{wch:16},{wch:16}];\n  XLSX.utils.book_append_sheet(wb,ws1,\'Resumen Ejecutivo\');\n\n  // Hoja 2\n  const h2=[\n    [`PRESCRIPCIONES POR CONSULTA — POR SERVICIO — ${periodo}`],[\'3 escenarios lado a lado\'],[],\n    [\'\',\'[ESC 1: COMPLETO]\',\'\',\'\',\'\',\'\',\'\',\'[ESC 2: SIN PAMI]\',\'\',\'\',\'\',\'\',\'\',\'[ESC 3: SIN PAMI NI LAB]\',\'\',\'\',\'\',\'\',\'\'],\n    [\'Servicio\',\'Consultas\',\'Con Presc\',\'Sin Presc\',\'% Con Presc\',\'Total Presc\',\'Ratio P/C\',\'Consultas\',\'Con Presc\',\'Sin Presc\',\'% Con Presc\',\'Total Presc\',\'Ratio P/C\',\'Consultas\',\'Con Presc\',\'Sin Presc\',\'% Con Presc\',\'Total Presc\',\'Ratio P/C\'],\n  ];\n  (data.por_servicio||[]).sort((a,b)=>d(b.esc1_consultas)-d(a.esc1_consultas)).forEach(s=>{\n    h2.push([s.servicio,d(s.esc1_consultas),d(s.esc1_con_presc),d(s.esc1_sin_presc),semP((s.esc1_pct||0)/100),d(s.esc1_total_presc),semR(s.esc1_ratio),d(s.esc2_consultas),d(s.esc2_con_presc),d(s.esc2_sin_presc),semP((s.esc2_pct||0)/100),d(s.esc2_total_presc),semR(s.esc2_ratio),d(s.esc3_consultas),d(s.esc3_con_presc),d(s.esc3_sin_presc),semP((s.esc3_pct||0)/100),d(s.esc3_total_presc),semR(s.esc3_ratio)]);\n  });\n  const ws2=XLSX.utils.aoa_to_sheet(h2);\n  ws2[\'!cols\']=[{wch:32},...Array(18).fill({wch:14})];\n  XLSX.utils.book_append_sheet(wb,ws2,\'Por Servicio\');\n\n  // Hoja 3\n  const h3=[\n    [`PRESCRIPCIONES POR CONSULTA — POR PROFESIONAL — ${periodo}`],[],\n    [\'\',\'\',\'[ESC 1: COMPLETO]\',\'\',\'\',\'\',\'\',\'\',\'[ESC 2: SIN PAMI]\',\'\',\'\',\'\',\'\',\'\',\'[ESC 3: SIN PAMI NI LAB]\',\'\',\'\',\'\',\'\',\'\'],\n    [\'Profesional\',\'Servicio\',\'Consultas\',\'Con Presc\',\'Sin Presc\',\'% Con Presc\',\'Total Presc\',\'Ratio P/C\',\'Consultas\',\'Con Presc\',\'Sin Presc\',\'% Con Presc\',\'Total Presc\',\'Ratio P/C\',\'Consultas\',\'Con Presc\',\'Sin Presc\',\'% Con Presc\',\'Total Presc\',\'Ratio P/C\'],\n  ];\n  (data.por_profesional||[]).sort((a,b)=>d(b.esc1_consultas)-d(a.esc1_consultas)).forEach(p=>{\n    h3.push([p.profesional,p.servicio,d(p.esc1_consultas),d(p.esc1_con_presc),d(p.esc1_sin_presc),semP((p.esc1_pct||0)/100),d(p.esc1_total_presc),semR(p.esc1_ratio),d(p.esc2_consultas),d(p.esc2_con_presc),d(p.esc2_sin_presc),semP((p.esc2_pct||0)/100),d(p.esc2_total_presc),semR(p.esc2_ratio),d(p.esc3_consultas),d(p.esc3_con_presc),d(p.esc3_sin_presc),semP((p.esc3_pct||0)/100),d(p.esc3_total_presc),semR(p.esc3_ratio)]);\n  });\n  const ws3=XLSX.utils.aoa_to_sheet(h3);\n  ws3[\'!cols\']=[{wch:32},{wch:28},...Array(18).fill({wch:13})];\n  XLSX.utils.book_append_sheet(wb,ws3,\'Por Profesional\');\n\n  // Hoja 4\n  const h4=[[\'TIPOS DE ESTUDIO/PRÁCTICA POR SERVICIO — ESC 3: SIN PAMI NI LAB\'],[`${periodo} | ${centro}`],[],[\'Servicio\',\'Tipo de Estudio / Práctica\',\'Consultas Episodio\',\'Prescripciones\',\'Ratio P/C\']];\n  let last4=\'\';\n  (data.tipos_por_servicio_esc3||[]).sort((a,b)=>{if(a.servicio<b.servicio)return -1;if(a.servicio>b.servicio)return 1;return d(b.prescripciones)-d(a.prescripciones)}).forEach(t=>{\n    const svc=t.servicio!==last4?t.servicio:\'\';last4=t.servicio;\n    h4.push([svc,t.tipo,d(t.consultas_episodio),d(t.prescripciones),semR(t.ratio)]);\n  });\n  const ws4=XLSX.utils.aoa_to_sheet(h4);\n  ws4[\'!cols\']=[{wch:30},{wch:38},{wch:20},{wch:18},{wch:16}];\n  XLSX.utils.book_append_sheet(wb,ws4,\'Tipos x Servicio (Esc.3)\');\n\n  // Hoja 5\n  const h5=[[\'TIPOS DE ESTUDIO/PRÁCTICA POR PROFESIONAL — ESC 3\'],[`${periodo} | ${centro} | 🟢%SinPresc<25% 🟡25-50% 🔴≥75%`],[],[\'Profesional\',\'Servicio\',\'Tipo / Resumen\',\'Consultas Episodio\',\'Prescripciones\',\'Ratio P/C\',\'% Sin Presc\']];\n  (data.tipos_por_profesional_esc3||[]).forEach(t=>{\n    const sin=t.es_resumen&&typeof t.sin_presc_pct===\'number\'?semPS(t.sin_presc_pct/100):\'\';\n    h5.push([t.profesional||\'\',t.servicio||\'\',t.tipo||\'\',d(t.consultas_episodio),d(t.prescripciones),semR(t.ratio),sin]);\n  });\n  const ws5=XLSX.utils.aoa_to_sheet(h5);\n  ws5[\'!cols\']=[{wch:32},{wch:28},{wch:36},{wch:18},{wch:16},{wch:16},{wch:14}];\n  XLSX.utils.book_append_sheet(wb,ws5,\'Tipos x Profesional (Esc.3)\');\n\n  // Hoja 6\n  const h6=[[\'DURACIÓN DE CONSULTA vs. PRESCRIPCIÓN\'],[`${periodo} | ${centro} | Excluidos: <5 min y >90 min`],[],[\'Rango Duración\',\'N° Consultas\',\'Con Presc\',\'Sin Presc\',\'% Con Presc\',\'Ratio P/C\',\'Duración Prom\']];\n  (data.duracion_vs_prescripcion||[]).forEach(dv=>{\n    h6.push([dv.rango,d(dv.consultas),d(dv.con_presc),d(dv.sin_presc),semP((dv.pct_con_presc||0)/100),semR(dv.ratio),typeof dv.duracion_prom===\'number\'?dv.duracion_prom.toFixed(0)+\' min\':\'—\']);\n  });\n  const ws6=XLSX.utils.aoa_to_sheet(h6);\n  ws6[\'!cols\']=[{wch:16},{wch:14},{wch:12},{wch:12},{wch:14},{wch:14},{wch:14}];\n  XLSX.utils.book_append_sheet(wb,ws6,\'Duracion vs Prescripcion\');\n\n  // Hoja 7\n  const h7=[\n    [`DESVÍOS Y OPORTUNIDADES DE MEJORA — ${periodo}`],[`Centro: ${centro}`],[],\n    [\'TOP 12 PROFESIONALES POR RATIO — Escenario 3 (Sin PAMI ni Laboratorio)\'],[],\n    [\'Profesional\',\'Servicio\',\'Ratio P/C\',\'Consultas\',\'Total Presc\'],\n  ];\n  (data.desvios_top12||[]).forEach(dv=>{h7.push([dv.profesional,dv.servicio,semR(dv.esc3_ratio),d(dv.esc3_consultas),d(dv.esc3_total_presc)])});\n  if((data.servicios_sin_prescripcion_esc3||[]).length){\n    h7.push([],[],[\'SERVICIOS SIN PRESCRIPCIONES — Escenario 3\'],[]);\n    data.servicios_sin_prescripcion_esc3.forEach(s=>h7.push([s]));\n  }\n  if(data.notas){h7.push([],[\'HALLAZGOS Y NOTAS\'],[data.notas])}\n  const ws7=XLSX.utils.aoa_to_sheet(h7);\n  ws7[\'!cols\']=[{wch:36},{wch:28},{wch:14},{wch:13},{wch:14}];\n  XLSX.utils.book_append_sheet(wb,ws7,\'Desvios y Oportunidades\');\n\n  return XLSX.write(wb,{type:\'array\',bookType:\'xlsx\'});\n}\n</script>\n</body>\n</html>\n'

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def serve_app():
    return HTML_APP
