"""
Motor de análisis de conducta prescriptiva — procesamiento Python puro.
Recibe dos archivos Excel (bytes) y devuelve el dict de resultado.
"""
import io
import re
import pandas as pd
import numpy as np
from datetime import timedelta

CODIGOS_CONSULTA = {'429926','420101','429965','429921','420405',
                    '429957','429950','429901','420701','429949'}
PAMI_CONVENIOS   = {'PAMI SDLC','PAMI OFTAL'}


# ── Clasificación de 28 categorías ───────────────────────────────────────────

def clasificar(prestacion: str) -> str:
    p = str(prestacion).upper()

    # Exclusiones específicas primero
    es_taco    = 'TACO' in p and ('HISTOL' in p or 'TACO HISTOL' in p)
    es_oct     = 'OCT' in p or 'COHERENCIA OPTICA' in p or 'TOMOGRAFIA DE COHERENCIA' in p
    es_eritro  = 'ERITROSEDIMENTACION' in p or 'ERITROSEDIMENT' in p
    es_para    = 'PARATHORMONA' in p or 'PTH' in p
    es_fondo   = 'FONDO DE OJO' in p
    es_eeg     = 'ELECTROENCEFALOGRAM' in p or 'ELECTROENCEFALO' in p or ' EEG' in p

    # 3. Ecocardiograma
    if 'ECOCARDIO' in p or ('ECO' in p and 'CARDIO' in p):
        return 'Ecocardiograma'

    # 4. Ecodoppler
    if 'ECODOPPLER' in p or 'DOPPLER' in p:
        return 'Ecodoppler'

    # 1. Ecografía ginecológica
    if 'ECO' in p and not es_taco:
        gine_kw = ['TOCOGINE','VAGINAL','MAMARI','OBSTET','PELVI','GENITO',
                   'ENDOMETRI','UTERINA','UTERO','OVARI','GINECOL','TOCO']
        if any(k in p for k in gine_kw):
            return 'Ecografia ginecologica'
        return 'Ecografia'

    # 5. TAC
    if ('TOMOGRAFIA' in p or 'ANGIOTOMOGRAFIA' in p or 'PIELOTAC' in p) and not es_oct and not es_eritro and not es_para and not es_fondo and not es_taco:
        return 'TAC'

    # 6. RNM
    if 'RESONANCIA' in p or 'ANGIORESONANCIA' in p or 'COLANGIORESONANCIA' in p:
        return 'RNM'

    # 7. Radiografía
    if re.search(r'\bRX\b', p) or 'RADIOGRAFIA' in p or 'ESPINOGRAFIA' in p or 'RADIOGRAF' in p:
        return 'Radiografia'

    # 8. Mamografía
    if 'MAMOGRAFIA' in p or 'SENOGRAFIA' in p or 'MAMOGRAF' in p:
        return 'Mamografia'

    # 9. Densitometría
    if 'DENSITOMETRIA' in p or 'DENSITOMETR' in p:
        return 'Densitometria'

    # 10. Medicina nuclear
    if any(k in p for k in ['CENTELLOGRAFIA','GAMMAGRAFIA','SPECT','PET-CT','PET CT',' PET ']):
        return 'Medicina nuclear'

    # 11. Holter/MAPA/Ergometría
    if any(k in p for k in ['HOLTER','MAPA','PRESUROMETRIA','ERGOMETRIA','MONITOREO AMBULATORIO']):
        return 'Holter/MAPA/Ergometria'

    # 12. ECG
    if ('ELECTROCARDIOGRAMA' in p or 'TELEMETRIA' in p) and not es_eeg:
        return 'ECG'

    # 16. Estudio neurológico (incluye EEG)
    if es_eeg or 'ELECTROMIOGRAFIA' in p or 'VELOCIDAD DE CONDUCCION' in p or \
       'POTENCIAL EVOCADO' in p or 'VIDEONISTAGMOGRAFIA' in p:
        return 'Estudio neurologico'

    # 13. Espirometría
    if 'ESPIROMETRIA' in p or 'BRONCOESPIROMETRIA' in p or 'ESPIROMET' in p:
        return 'Espirometria'

    # 14. Estudio del sueño
    if 'POLISOMNOGRAFIA' in p or 'POLIGRAFIA RESPIRATORIA' in p or 'SUENO' in p or 'SUEÑO' in p:
        return 'Estudio del sueno'

    # 15. Estudio oftalmológico
    if any(k in p for k in ['CAMPIMETRIA','CAMPO VISUAL','OFTALMOSCOPIA','RETINOFLUORESCEI',
                              'TOPOGRAFIA CORNEAL','PAQUIMETRIA','ECOMETRIA','RETINOGRAFIA',
                              'RECUENTO ENDOTELIO','ESQUIASCOPIA','OFTALMOL']) or es_oct or es_fondo:
        return 'Estudio oftalmologico'

    # 17. Audiología
    if any(k in p for k in ['AUDIOMETRIA','LOGOAUDIOMETRIA','IMPEDANCIOMETRIA','OTOEMISIONES',
                              'OTOMICROSCOPIA','ACUFENOMETRIA','TIMPANOMETRIA','TEST DE OLFATO',
                              'FONOAUDIOLOGIA','REHABILITACION DEL LENGUAJE']):
        return 'Audiologia'

    # 18. Endoscopía
    if any(k in p for k in ['ENDOSCOPIA','COLONOSCOPIA','GASTROSCOPIA','VIDEOCOLON',
                              'VIDEOESOFAGO','FIBROSCOPIA','FIBROSCAN','CISTOURETROFIBROSCOPIA',
                              'URETROCISTOSCOPIA','CISTOURETROGRAFIA','POLIPECTOMIA ENDOSCOPICA',
                              'FARINGO','RINO-SINUSO']):
        return 'Endoscopia'

    # 19. Estudio ginecológico (no eco)
    citol_exc = 'CITOLOGICO COMPLETO' in p or 'HISTOGRAMA' in p or \
                ('CITOLOGIA' in p and ('LIQUIDO' in p or 'ORINA' in p or 'EXFOLIATIVA' in p))
    if not citol_exc and any(k in p for k in ['COLPOSCOPIA','PAPANICOLAU','HISTEROSCOPIA',
                                               'HISTEROSALPINGOGRAFIA','VULVOSCOPIA',
                                               'CITOLOGIA ONCOLOGICA','CITOLOGICO ONCOL']):
        return 'Estudio ginecologico (no eco)'

    # 21. Biopsia/Anat.Patológica
    if any(k in p for k in ['BIOPSIA','MEDULOGRAMA','MIELOGRAMA','TACO HISTOL',
                              'RECEPTORES HORMONALES','INMUNOHISTOQUIMICA']) or es_taco:
        return 'Biopsia/Anat.Patologica'

    # 20. Práctica quirúrgica
    quirurgicos = ['COLECISTECTOMIA','HISTERECTOMIA','HERNIORRAFIA','HEMICOLECTOMIA',
                   'HEMORROIDECTOMIA','FISTULECTOMIA','MASTOPLASTIA','DERMOLIPECTOMIA',
                   'VASECTOMIA','LIGADURA TUBARIA','ARTROSCOPIA','CONIZACION',
                   'RECONSTRUCCION PABELLON','ESCISION DE CUADRANTE','NEFROLITOTOMIA',
                   'URETERORRENOSCOPIA','RTU ','SEPTUMPLASTIA','TURBINECTOMIA','SINUSOTOMIA',
                   'COLANGIOPANCREATOGRAFIA','ENTEROLISIS','CORONARIOGRAFIA',
                   'COLOCACION DE DIU','EXTRACCION DE DIU','IMPLANTE SUBDERMICO',
                   'PUNCION BIOPSIA GUIADA','GANGLIO CENTINELA','ANESTESIOLOGIA',
                   'EXTRACCION CUERPO EXTRANO','FRENULOTOMIA','DESCOMPRESION DEL MEDIANO',
                   'TUNEL CARPIANO','CIRUGIA ','CIRUGÍA ','LIPECTOMIA','BLEFAROPLASTIA',
                   'RINOPLASTIA','OTOPLASTIA','ABDOMINOPLASTIA','SUTURA DE HERIDA',
                   'CIERRE PLASTICO','CAPSULOTOMIA','ESCISION DE LESION','DESTRUCCION DE LESION',
                   'EXTIRPACION','RESECCION','APENDICECTOMIA','TIROIDECTOMIA',
                   'PARATIROIDECTOMIA','PROSTATECTOMIA','NEFRECTOMIA']
    if any(k in p for k in quirurgicos):
        return 'Practica quirurgica'

    # 22. Kinesiología
    if any(k in p for k in ['FISIOTERAPIA','KINESIOTERAPIA','KINESIO','TERAPIA FISICA',
                              'AGENTES FISICOS','HIDROTERAPIA','PARAFINA','CRIOTERAPIA',
                              'ELECTROTERAPIA','ULTRASONIDO TERAPEUTICO','TRACCION CERVICAL']):
        return 'Kinesiologia/Rehabilitacion'

    # 23. Salud mental
    if any(k in p for k in ['PSICOLOGIA','PSICOTERAPIA','PSIQUIATRIA','PSICOPEDAGOGIA',
                              'SESION DE PSICO','ATENCION PSICOL']):
        return 'Salud mental'

    # 24. Procedimiento terapéutico
    if any(k in p for k in ['TRANSFUSION','INFUSION IV','VENOCLISIS','QUIMIOTERAPIA',
                              'MODULO DE QUIMIO','HIDRATACION IV']):
        return 'Procedimiento terapeutico'

    # 25. Estudio funcional
    if any(k in p for k in ['URODINAMICA','URODINAMIA','UROFLUJOMETRIA','TEST DE CAMINATA',
                              'FRACCION EXHALADA','FENO','DINAMICA DEL TRANSITO']):
        return 'Estudio funcional'

    # 26. Estudio urológico/reproductivo
    if 'ESPERMOGRAMA' in p:
        return 'Estudio urologico/reproduc.'

    # 27. Consulta/Interconsulta
    if any(k in p for k in ['CONSULTA','INTERCONSULTA']):
        return 'Consulta/Interconsulta'

    # 28. Laboratorio (todo lo restante)
    return 'Laboratorio'


# ── Motor principal ───────────────────────────────────────────────────────────

def analizar_excel(bytes_atenciones: bytes, bytes_prescripciones: bytes) -> dict:
    # Leer archivos
    df_at = pd.read_excel(io.BytesIO(bytes_atenciones))
    df_pr = pd.read_excel(io.BytesIO(bytes_prescripciones))

    # Detectar centro
    centro = 'Todos los centros'
    if 'CENTRO' in df_at.columns:
        centros = df_at['CENTRO'].dropna().unique()
        if len(centros) == 1:
            centro = centros[0]
        elif len(centros) > 1:
            centro = df_at['CENTRO'].mode()[0]

    # Normalizar columnas atenciones
    df_at['fecha']   = pd.to_datetime(df_at['FECHA_CAL'], errors='coerce').dt.date
    df_at['pac']     = df_at['APELLIDO_NOMBRE_PAC'].fillna('').str.strip().str.upper()
    df_at['prof']    = df_at['PROFESIONAL'].fillna('').str.strip().str.upper()
    df_at['servicio']= df_at['SERVICIO'].fillna('').str.strip().str.upper()
    df_at['convenio']= df_at['CONVENIO'].fillna('').str.strip().str.upper()
    df_at['cod']     = df_at['COD_PRESTACION'].astype(str).str.strip()

    # Duración en minutos
    def to_min(v):
        if isinstance(v, timedelta): return v.total_seconds() / 60
        try: return float(v) / 60
        except: return None
    df_at['duracion_min'] = df_at['DURACION'].apply(to_min)

    # Normalizar columnas prescripciones
    df_pr['fecha']    = pd.to_datetime(df_pr['Fecha Emisión'], errors='coerce').dt.date
    df_pr['pac']      = df_pr['Paciente'].fillna('').str.strip().str.upper()
    df_pr['prof']     = df_pr['Prescriptor'].fillna('').str.strip().str.upper()
    df_pr['obra_soc'] = df_pr['Obra Social'].fillna('').str.strip().str.upper()
    df_pr['prestacion']= df_pr['Prestación'].fillna('')
    df_pr['tipo']     = df_pr['prestacion'].apply(clasificar)

    # Excluir vacunas
    vac = df_pr['prestacion'].str.upper().str.contains('VACU|ANTIGRIPAL', na=False)
    df_pr = df_pr[~vac].copy()

    # Filtrar consultas válidas
    consultas = df_at[
        df_at['cod'].isin(CODIGOS_CONSULTA) & df_at['fecha'].notna()
    ].copy()

    # Episodios únicos (pac + prof + fecha) — con servicio y convenio del primero
    ep = consultas.groupby(['pac','prof','fecha']).agg(
        servicio=('servicio','first'),
        convenio=('convenio','first'),
        duracion_min=('duracion_min','mean'),
    ).reset_index()

    def calcular_escenario(ep_df, pr_df):
        if len(ep_df) == 0:
            return {'total_consultas':0,'con_prescripcion':0,'sin_prescripcion':0,
                    'pct_con_presc':0.0,'total_prescripciones':0,'ratio':0.0}
        merged = ep_df.merge(
            pr_df[['pac','prof','fecha','prestacion','tipo']],
            on=['pac','prof','fecha'], how='left'
        )
        total_ep   = len(ep_df)
        total_presc= merged['prestacion'].notna().sum()
        ep_con     = merged[merged['prestacion'].notna()].groupby(['pac','prof','fecha']).ngroups
        return {
            'total_consultas':    int(total_ep),
            'con_prescripcion':   int(ep_con),
            'sin_prescripcion':   int(total_ep - ep_con),
            'pct_con_presc':      round(ep_con/total_ep*100, 1) if total_ep else 0.0,
            'total_prescripciones': int(total_presc),
            'ratio':              round(total_presc/total_ep, 2) if total_ep else 0.0,
        }

    # PAMI flags
    pami_pac_prof_fecha = set(
        zip(ep[ep['convenio'].isin(PAMI_CONVENIOS)]['pac'],
            ep[ep['convenio'].isin(PAMI_CONVENIOS)]['prof'],
            ep[ep['convenio'].isin(PAMI_CONVENIOS)]['fecha'])
    )
    ep_nopami = ep[~ep['convenio'].isin(PAMI_CONVENIOS)].copy()
    pr_nopami = df_pr[~df_pr['obra_soc'].isin(PAMI_CONVENIOS)].copy()
    pr_nolab  = pr_nopami[pr_nopami['tipo'] != 'Laboratorio'].copy()

    esc1 = calcular_escenario(ep, df_pr)
    esc2 = calcular_escenario(ep_nopami, pr_nopami)
    esc3 = calcular_escenario(ep_nopami, pr_nolab)

    # ── Por Servicio ──────────────────────────────────────────────────────────
    def por_servicio_esc(ep_df, pr_df):
        if len(ep_df) == 0: return {}
        merged = ep_df.merge(pr_df[['pac','prof','fecha','prestacion']], on=['pac','prof','fecha'], how='left')
        grp = merged.groupby('servicio')
        resultado = {}
        for svc, g in grp:
            ep_u = g.drop_duplicates(['pac','prof','fecha'])
            tot  = len(ep_u)
            presc= g['prestacion'].notna().sum()
            con  = g[g['prestacion'].notna()].drop_duplicates(['pac','prof','fecha']).__len__()
            resultado[svc] = {
                'consultas': int(tot), 'con_presc': int(con),
                'sin_presc': int(tot-con),
                'pct': round(con/tot*100,1) if tot else 0.0,
                'total_presc': int(presc),
                'ratio': round(presc/tot,2) if tot else 0.0,
            }
        return resultado

    svc1 = por_servicio_esc(ep, df_pr)
    svc2 = por_servicio_esc(ep_nopami, pr_nopami)
    svc3 = por_servicio_esc(ep_nopami, pr_nolab)
    todos_servicios = sorted(set(list(svc1.keys())+list(svc2.keys())+list(svc3.keys())))

    por_servicio = []
    for svc in todos_servicios:
        def g(d, k): return d.get(svc, {}).get(k, '—')
        por_servicio.append({
            'servicio': svc,
            'esc1_consultas': g(svc1,'consultas'), 'esc1_con_presc': g(svc1,'con_presc'),
            'esc1_sin_presc': g(svc1,'sin_presc'), 'esc1_pct': g(svc1,'pct'),
            'esc1_total_presc': g(svc1,'total_presc'), 'esc1_ratio': g(svc1,'ratio'),
            'esc2_consultas': g(svc2,'consultas'), 'esc2_con_presc': g(svc2,'con_presc'),
            'esc2_sin_presc': g(svc2,'sin_presc'), 'esc2_pct': g(svc2,'pct'),
            'esc2_total_presc': g(svc2,'total_presc'), 'esc2_ratio': g(svc2,'ratio'),
            'esc3_consultas': g(svc3,'consultas'), 'esc3_con_presc': g(svc3,'con_presc'),
            'esc3_sin_presc': g(svc3,'sin_presc'), 'esc3_pct': g(svc3,'pct'),
            'esc3_total_presc': g(svc3,'total_presc'), 'esc3_ratio': g(svc3,'ratio'),
        })

    # ── Por Profesional ───────────────────────────────────────────────────────
    def por_prof_esc(ep_df, pr_df):
        if len(ep_df) == 0: return {}
        merged = ep_df.merge(pr_df[['pac','prof','fecha','prestacion']], on=['pac','prof','fecha'], how='left')
        resultado = {}
        for (prof, svc), g in merged.groupby(['prof','servicio']):
            ep_u = g.drop_duplicates(['pac','prof','fecha'])
            tot  = len(ep_u)
            presc= g['prestacion'].notna().sum()
            con  = g[g['prestacion'].notna()].drop_duplicates(['pac','prof','fecha']).__len__()
            resultado[(prof,svc)] = {
                'consultas': int(tot), 'con_presc': int(con),
                'sin_presc': int(tot-con),
                'pct': round(con/tot*100,1) if tot else 0.0,
                'total_presc': int(presc),
                'ratio': round(presc/tot,2) if tot else 0.0,
            }
        return resultado

    pr1 = por_prof_esc(ep, df_pr)
    pr2 = por_prof_esc(ep_nopami, pr_nopami)
    pr3 = por_prof_esc(ep_nopami, pr_nolab)
    todos_profs = sorted(set(list(pr1.keys())+list(pr2.keys())+list(pr3.keys())))

    por_profesional = []
    for (prof, svc) in todos_profs:
        def gp(d, k): return d.get((prof,svc), {}).get(k, '—')
        por_profesional.append({
            'profesional': prof, 'servicio': svc,
            'esc1_consultas': gp(pr1,'consultas'), 'esc1_con_presc': gp(pr1,'con_presc'),
            'esc1_sin_presc': gp(pr1,'sin_presc'), 'esc1_pct': gp(pr1,'pct'),
            'esc1_total_presc': gp(pr1,'total_presc'), 'esc1_ratio': gp(pr1,'ratio'),
            'esc2_consultas': gp(pr2,'consultas'), 'esc2_con_presc': gp(pr2,'con_presc'),
            'esc2_sin_presc': gp(pr2,'sin_presc'), 'esc2_pct': gp(pr2,'pct'),
            'esc2_total_presc': gp(pr2,'total_presc'), 'esc2_ratio': gp(pr2,'ratio'),
            'esc3_consultas': gp(pr3,'consultas'), 'esc3_con_presc': gp(pr3,'con_presc'),
            'esc3_sin_presc': gp(pr3,'sin_presc'), 'esc3_pct': gp(pr3,'pct'),
            'esc3_total_presc': gp(pr3,'total_presc'), 'esc3_ratio': gp(pr3,'ratio'),
        })

    # ── Tipos x Servicio (Esc 3) ──────────────────────────────────────────────
    merged3 = ep_nopami.merge(pr_nolab[['pac','prof','fecha','prestacion','tipo']],
                               on=['pac','prof','fecha'], how='inner')
    tipos_svc = merged3.groupby(['servicio','tipo']).agg(
        consultas_episodio=('pac', lambda x: ep_nopami[
            ep_nopami['prof'].isin(merged3[merged3['tipo']==x.name[1] if hasattr(x,'name') else '']['prof'])
        ]['pac'].nunique()),
        prescripciones=('prestacion','count')
    ).reset_index()

    # Simplificar: episodios del servicio y prescripciones por tipo
    svc_ep_count = ep_nopami.groupby('servicio').size().to_dict()
    tipo_svc_presc = merged3.groupby(['servicio','tipo'])['prestacion'].count().reset_index()
    tipos_por_servicio_esc3 = []
    for _, row in tipo_svc_presc.iterrows():
        svc = row['servicio']; tipo = row['tipo']; presc = int(row['prestacion'])
        ep_svc = svc_ep_count.get(svc, 0)
        tipos_por_servicio_esc3.append({
            'servicio': svc, 'tipo': tipo,
            'consultas_episodio': ep_svc,
            'prescripciones': presc,
            'ratio': round(presc/ep_svc, 2) if ep_svc else 0.0,
        })
    tipos_por_servicio_esc3.sort(key=lambda x: (x['servicio'], -x['prescripciones']))

    # ── Tipos x Profesional (Esc 3) ───────────────────────────────────────────
    prof_ep_count = ep_nopami.groupby(['prof','servicio']).size().to_dict()
    tipo_prof_presc = merged3.groupby(['prof','servicio','tipo'])['prestacion'].count().reset_index()
    tipos_por_profesional_esc3 = []
    for _, row in tipo_prof_presc.iterrows():
        prof=row['prof']; svc=row['servicio']; tipo=row['tipo']; presc=int(row['prestacion'])
        ep_p = prof_ep_count.get((prof,svc), 0)
        tipos_por_profesional_esc3.append({
            'profesional': prof, 'servicio': svc, 'tipo': tipo,
            'consultas_episodio': ep_p,
            'prescripciones': presc,
            'ratio': round(presc/ep_p, 2) if ep_p else 0.0,
            'es_resumen': False, 'sin_presc_pct': 0.0,
        })
    tipos_por_profesional_esc3.sort(key=lambda x: (x['servicio'], x['profesional'], -x['prescripciones']))

    # ── Duración vs Prescripción ──────────────────────────────────────────────
    rangos = [
        ('<10',    0,   10),
        ('10-20', 10,   20),
        ('20-30', 20,   30),
        ('30-45', 30,   45),
        ('45-60', 45,   60),
        ('>60',   60, 9999),
    ]
    labels = {'<10':'< 10 min','10-20':'10-20 min','20-30':'20-30 min',
              '30-45':'30-45 min','45-60':'45-60 min','>60':'> 60 min'}

    merged_dur = ep.merge(df_pr[['pac','prof','fecha','prestacion']], on=['pac','prof','fecha'], how='left')
    duracion_vs = []
    for (rk, rmin, rmax) in rangos:
        mask = (merged_dur['duracion_min'] > 5) & \
               (merged_dur['duracion_min'] <= 90) & \
               (merged_dur['duracion_min'] >= rmin) & \
               (merged_dur['duracion_min'] < rmax)
        g = merged_dur[mask]
        ep_u = g.drop_duplicates(['pac','prof','fecha'])
        tot  = len(ep_u)
        presc= g['prestacion'].notna().sum()
        con  = g[g['prestacion'].notna()].drop_duplicates(['pac','prof','fecha']).__len__()
        dp   = g['duracion_min'].mean()
        duracion_vs.append({
            'rango': labels[rk],
            'consultas': int(tot), 'con_presc': int(con),
            'sin_presc': int(tot-con),
            'pct_con_presc': round(con/tot*100,1) if tot else 0.0,
            'ratio': round(presc/tot,2) if tot else 0.0,
            'duracion_prom': round(dp,1) if not np.isnan(dp) else 0.0,
        })

    # ── Desvíos top 12 ────────────────────────────────────────────────────────
    desvios = sorted(
        [p for p in por_profesional if isinstance(p.get('esc3_ratio'), float) and p['esc3_ratio'] > 0],
        key=lambda x: -x['esc3_ratio']
    )[:12]
    desvios_top12 = [{
        'profesional': d['profesional'], 'servicio': d['servicio'],
        'esc3_ratio': d['esc3_ratio'],
        'esc3_consultas': d['esc3_consultas'],
        'esc3_total_presc': d['esc3_total_presc'],
    } for d in desvios]

    # Servicios sin prescripción en Esc3
    sin_presc = [s['servicio'] for s in por_servicio
                 if isinstance(s.get('esc3_total_presc'), int) and s['esc3_total_presc'] == 0
                 and isinstance(s.get('esc3_consultas'), int) and s['esc3_consultas'] > 0]

    return {
        'periodo': '',
        'centro': centro,
        'escenario_1': dict(nombre='Completo', **esc1),
        'escenario_2': dict(nombre='Sin PAMI', **esc2),
        'escenario_3': dict(nombre='Sin PAMI ni Laboratorio', **esc3),
        'por_servicio': por_servicio,
        'por_profesional': por_profesional,
        'tipos_por_servicio_esc3': tipos_por_servicio_esc3,
        'tipos_por_profesional_esc3': tipos_por_profesional_esc3,
        'duracion_vs_prescripcion': duracion_vs,
        'desvios_top12': desvios_top12,
        'servicios_sin_prescripcion_esc3': sin_presc,
        'notas': 'Análisis generado automáticamente con motor Python. Join: Paciente+Profesional+Fecha.',
    }
