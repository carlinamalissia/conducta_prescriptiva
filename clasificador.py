"""
Clasificador de prácticas y filtros de escenarios.
Centraliza toda la lógica de clasificación para que sea fácil de mantener.
"""

CODIGOS_CONSULTA = {
    429926, 420101, 429965, 429921, 420405,
    429957, 429950, 429901, 420701, 429949
}

KEYWORDS_PAMI = ["PAMI"]

KEYWORDS_LAB = [
    "GLUCEMIA", "GLUCOSA", "HEMOGLUCOTEST",
    "CREATININA", "UREA", "URICEMIA", "ACIDO URICO",
    "COLESTEROL", "TRIGLICERIDOS", "LIPOPROTEINA", "APOPROTEINA",
    "ALBUMINURIA", "ALBUMINA",
    "PROTEINA C REACTIVA", "PCR ",
    "CITOLOGICO", "CITOLOGIA EXFOLIATIVA",
    "HEMOGRAMA", "HEMATOCRITO", "HEMOGLOBINA", "ERITROSEDIMENTACION", "VSG",
    "IONOGRAMA", "SODIO", "POTASIO", "CLORO ", "CALCIO IONICO",
    "ORINA COMPLETA", "UROCULTIVO", "SEDIMENTO URINARIO",
    "TSH", "TIROTROFINA", "TIROXINA", "TRIYODOTIRONINA", "PARATHORMONA",
    "HEPATITIS", "HIV", "ANTICUERPO ANTI",
    "CULTIVO", "HEMOCULTIVO", "ANTIBIOGRAMA",
    "COAGULACION", "TIEMPO DE PROTROMBINA", "APTT", "FIBRINOGENO",
    "FERRITINA", "HIERRO SERICO", "TRANSFERRINA",
    "VITAMINA B", "VITAMINA D", "ACIDO FOLICO",
    "INSULINA", "PEPTIDO C", "MICROALBUMINURIA",
    "CLEARANCE", "PROTEINURIA",
    "LDH", "CPK", "TROPONINA", "PRO BNP",
    "AMILASA", "LIPASA", "BILIRRUBINA", "TGO", "TGP", "TRANSAMINASA",
    "FOSFATASA", "GGT", "PROTEINAS TOTALES",
    "ACIDO LACTICO", "GASES EN SANGRE", "GASOMETRIA",
    "SEROLOGIA", "CHAGAS", "TOXOPLASMA", "RUBEOLA", "SIFILIS", "VDRL",
    "ERITROPOYETINA", "PSA", "CEA", "CA 125", "CA 19",
    "INMUNOGLOBULINA", "COMPLEMENTO",
    "ERITROSEDIMENTACION",
]

CATEGORIAS_LAB_EXACTAS = {
    "ACTO BIOQUIMICO",
    "PANEL BIOQUIMICO",
}


def es_pami(convenio_u_obra_social: str) -> bool:
    if not convenio_u_obra_social:
        return False
    upper = str(convenio_u_obra_social).upper()
    return any(k in upper for k in KEYWORDS_PAMI)


def es_laboratorio(prestacion: str, codigo=None) -> bool:
    if not prestacion:
        return False
    upper = str(prestacion).upper().strip()
    if upper in CATEGORIAS_LAB_EXACTAS:
        return True
    return any(k in upper for k in KEYWORDS_LAB)


def es_consulta(codigo) -> bool:
    try:
        return int(codigo) in CODIGOS_CONSULTA
    except (ValueError, TypeError):
        return False


CATEGORIAS_PRACTICA = {
    "Consulta/Interconsulta": [
        "CONSULTA EN CONSULTORIO", "CONSULTA MEDICA", "CONSULTA ESPECIALISTA",
        "INTERCONSULTA", "CONSULTA EN GUARDIA", "CONSULTA CONTROL",
        "TURNO DIGITAL", "CONSULTA PRE", "CONSULTA POST", "CONSULTA 1ERA",
        "CONSULTA 2DA", "CONSULTA VALORACION",
    ],
    "Ecografia": [
        "ECOGRAFIA ABDOMINAL", "ECOGRAFIA RENAL", "ECOGRAFIA TIROIDEA",
        "ECOGRAFIA DE PARTES BLANDAS", "ECOGRAFIA DOPPLER",
        "ECOGRAFIA PELVIANA", "ECOGRAFIA VESICAL",
    ],
    "Ecografia ginecologica": [
        "ECOGRAFIA GINECOLOGICA", "ECOGRAFIA OBSTETRICA",
        "ECOGRAFIA TRANSVAGINAL", "ECOGRAFIA MAMARIA",
    ],
    "Ecodoppler": [
        "ECODOPPLER", "ECO DOPPLER",
    ],
    "Ecocardiograma": [
        "ECOCARDIOGRAMA",
    ],
    "Radiografia": [
        "RADIOGRAFIA", "RX ", "TELERRADIOGRAFIA",
    ],
    "TAC": [
        "TOMOGRAFIA", "TAC ", "TC ",
    ],
    "RNM": [
        "RESONANCIA", "RNM", "MRI",
    ],
    "Mamografia": [
        "MAMOGRAFIA", "MAMOGRAFIA BILATERAL",
    ],
    "Densitometria": [
        "DENSITOMETRIA", "DENSITOMETRIA OSEA",
    ],
    "Medicina nuclear": [
        "MEDICINA NUCLEAR", "CENTELLOGRAMA", "PET", "SPECT", "GAMMAGRAFIA",
        "CENTELLEOGRAFIA",
    ],
    "ECG": [
        "ELECTROCARDIOGRAMA", "ECG", "TRAZADO ELECTROCARDIOGRAFICO",
    ],
    "Holter/MAPA/Ergometria": [
        "HOLTER", "MAPA", "ERGOMETRIA", "MONITOREO AMBULATORIO",
        "TILT TEST", "PRUEBA DE ESFUERZO",
    ],
    "Espirometria": [
        "ESPIROMETRIA", "FUNCION RESPIRATORIA", "PLETISMOGRAFIA",
        "ESPIROGRAFIA",
    ],
    "Estudio funcional": [
        "ESTUDIO FUNCIONAL", "TEST DE MARCHA", "POLISOMNOGRAFIA",
        "URODINAMIA",
    ],
    "Estudio del sueno": [
        "ESTUDIO DE SUENO", "POLISOMNOGRAFIA", "POLIGRAF",
    ],
    "Estudio neurologico": [
        "ELECTROENCEFALOGRAMA", "EEG", "ELECTROMIOGRAFIA", "EMG",
        "POTENCIALES EVOCADOS", "VELOCIDAD DE CONDUCCION",
    ],
    "Audiologia": [
        "AUDIOMETRIA", "AUDIOLOGIA", "LOGOAUDIOMETRIA", "AUDIOGRAMA",
        "IMPEDANCIOMETRIA", "OTOEMISIONES", "POTENCIALES EVOCADOS AUDITIVOS",
    ],
    "Estudio oftalmologico": [
        "FONDO DE OJO", "CAMPIMETRIA", "TONOMETRIA", "OCT",
        "ANGIOGRAFIA RETINAL", "TOPOGRAFIA CORNEAL", "AGUDEZA VISUAL",
        "BIOMICROSCOPIA",
    ],
    "Estudio ginecologico (no eco)": [
        "COLPOSCOPIA", "PAPANICOLAOU", "PAP ", "HISTEROSCOPIA",
        "HISTEROSALPINGOGRAFIA", "BIOPSIA CERVICAL", "CERVICOGRAFIA",
    ],
    "Endoscopia": [
        "ENDOSCOPIA", "COLONOSCOPIA", "RECTOSCOPIA", "GASTROSCOPIA",
        "VIDEOENDOSCOPIA", "FIBROCOLONOSCOPIA", "FIBROGASTROSCOPIA",
        "LARINGOSCOPIA", "BRONCOSCOPIA",
    ],
    "Biopsia/Anat.Patologica": [
        "BIOPSIA", "ANATOMIA PATOLOGICA", "HISTOPATOLOGIA",
        "CITOLOGIA", "PUNCION",
    ],
    "Kinesiologia/Rehabilitacion": [
        "KINESIOLOGIA", "KINESIOTERAPIA", "REHABILITACION", "FISIOTERAPIA",
        "FONOAUDIOLOGIA", "TERAPIA OCUPACIONAL", "AGENTES FISICOS",
        "SESION DE", "SESIONES DE",
    ],
    "Salud mental": [
        "PSICOLOGIA", "PSIQUIATRIA", "PSICOTERAPIA", "SESION PSICOLOG",
        "CONSULTA PSIQ",
    ],
    "Practica quirurgica": [
        "CIRUGIA", "INTERVENCION QUIRURGICA", "PROCEDIMIENTO QUIRURGICO",
        "EXTIRPACION", "RESECCION", "PLASTIA",
    ],
    "Procedimiento terapeutico": [
        "INYECTABLE", "INFILTRACION", "CURACION", "VENDAJE",
        "COLOCACION DE", "RETIRO DE", "DRENAJE",
    ],
}


def clasificar_practica(prestacion: str) -> str:
    if not prestacion:
        return "Otro"
    upper = str(prestacion).upper().strip()
    if es_laboratorio(upper):
        return "Laboratorio"
    for categoria, keywords in CATEGORIAS_PRACTICA.items():
        if any(k in upper for k in keywords):
            return categoria
    return "Otro"
