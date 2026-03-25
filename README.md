# API Conducta Prescriptiva — GEA Sanatorios

Analiza automáticamente la conducta prescriptiva de médicos conectándose
al HIS (hospital.sdlc.com.ar), descargando los datos y procesando el cruce
consultas/prescripciones en 3 escenarios.

---

## Deploy en Railway (paso a paso)

### 1. Crear el repo en GitHub
```bash
git init
git add .
git commit -m "initial commit"
git remote add origin https://github.com/TU_USUARIO/prescriptiva-api.git
git push -u origin main
```

### 2. Crear el proyecto en Railway
1. Entrá a [railway.app](https://railway.app)
2. New Project → Deploy from GitHub repo → seleccioná el repo
3. Railway detecta el Dockerfile automáticamente

### 3. Configurar variable de entorno
En Railway → tu servicio → Variables:
```
API_KEY = una_clave_secreta_que_vos_eligas
```
Esta clave protege la API — la vas a usar en el header de cada request.

### 4. Deploy
Railway hace el build y deploy automáticamente.
La URL quedará algo como: `https://prescriptiva-api-production.up.railway.app`

Cada vez que hagas `git push`, Railway redeploya solo.

---

## Uso de la API

### Documentación interactiva
Una vez deployada, entrá a:
`https://TU-URL.railway.app/docs`

Ahí podés probar todos los endpoints directamente desde el browser.

---

### Endpoint: `/analizar` — devuelve JSON

```bash
curl -X POST https://TU-URL.railway.app/analizar \
  -H "Content-Type: application/json" \
  -H "X-API-Key: TU_CLAVE" \
  -d '{
    "usuario": "tu_usuario_his",
    "password": "tu_contraseña_his",
    "centro": "SANATORIO DE LA CAÑADA",
    "fecha_desde": "01/03/2026",
    "fecha_hasta": "31/03/2026"
  }'
```

Para todos los centros, dejá `"centro": ""`.

**Respuesta:**
```json
{
  "metadata": {
    "total_consultas_bruto": 11797,
    "total_prescripciones_bruto": 20346,
    "profesionales_unicos": 87,
    "centros": ["CENTRO CER+", "SANATORIO DE LA CAÑADA", ...],
    "servicios": ["CARDIOLOGIA", "CLINICA MEDICA", ...]
  },
  "escenario_1": {
    "nombre": "Completo",
    "total_consultas": 11797,
    "total_prescripciones": 20346,
    "ratio_global": 1.72,
    "por_profesional": [...],
    "por_servicio": [...],
    "por_centro": [...],
    "top_desvios": [...],
    "sin_prescripcion": [...]
  },
  "escenario_2": { ... },
  "escenario_3": { ... }
}
```

---

### Endpoint: `/exportar` — descarga el Excel

```bash
curl -X POST https://TU-URL.railway.app/exportar \
  -H "Content-Type: application/json" \
  -H "X-API-Key: TU_CLAVE" \
  -d '{
    "usuario": "tu_usuario_his",
    "password": "tu_contraseña_his",
    "centro": "SANATORIO DE LA CAÑADA",
    "fecha_desde": "01/03/2026",
    "fecha_hasta": "31/03/2026"
  }' \
  --output Prescriptiva_Marzo2026.xlsx
```

El Excel tiene 6 hojas:
- **Resumen** — KPIs comparativos por escenario
- **Esc1 — Completo**
- **Esc2 — Sin PAMI**
- **Esc3 — Sin PAMI ni Lab** — el más usado para análisis de desvíos
- **Sin prescripción** — médicos que atendieron pero no prescribieron
- **Desvíos** — top prescriptores con z-score y nivel de alerta

---

### Endpoint: `/profesional` — detalle de un médico

```bash
curl -X POST "https://TU-URL.railway.app/profesional?nombre_profesional=MANTELLI" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: TU_CLAVE" \
  -d '{
    "usuario": "tu_usuario_his",
    "password": "tu_contraseña_his",
    "centro": "",
    "fecha_desde": "01/03/2026",
    "fecha_hasta": "31/03/2026"
  }'
```

---

## Escenarios de análisis

| Escenario | Descripción |
|---|---|
| 1 — Completo | Todos los convenios, todas las prácticas incluyendo laboratorio |
| 2 — Sin PAMI | Excluye consultas y prescripciones de PAMI (PAMI SDLC y PAMI OFTAL) |
| 3 — Sin PAMI ni Lab | Escenario 2 + excluye laboratorio del numerador de prescripciones |

## Alertas por ratio (Escenario 3)

| Color | Criterio |
|---|---|
| Normal | ratio < media + 1 desvío estándar |
| Atención | ratio ≥ media + 1 desvío estándar |
| Crítico | ratio ≥ media + 2 desvíos estándar |

---

## Notas técnicas

- Las credenciales del HIS se usan solo durante la sesión y **nunca se almacenan**
- El scraper corre en modo headless (sin ventana visible)
- Máximo 1 mes por consulta (restricción del HIS)
- Tiempo estimado por request: 60-120 segundos (depende del volumen de datos)
- Railway ofrece HTTPS automático — las credenciales viajan cifradas
