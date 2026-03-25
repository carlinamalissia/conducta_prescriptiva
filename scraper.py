"""
Scraper para hospital.sdlc.com.ar (ThinkSoft JSF/PrimeFaces)
Descarga los dos Excel en memoria y los retorna como bytes.
"""

import asyncio
import io
import logging
from dataclasses import dataclass
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logger = logging.getLogger(__name__)

URL_LOGIN     = "https://hospital.sdlc.com.ar/HOSPITAL/pages/login.faces"
URL_INICIO    = "https://hospital.sdlc.com.ar/HOSPITAL/pages/inicio.faces"
URL_CONSULTAS = "https://hospital.sdlc.com.ar/HOSPITAL/pages/estadisticas/consulta/consultaAtenciones.faces"
URL_PRESCRIPCIONES = "https://hospital.sdlc.com.ar/HOSPITAL/pages/panelDeControl/atencionesMedicas/consultaPrescripcionesEmitidas.faces"


@dataclass
class ParamsDescarga:
    usuario: str
    password: str
    centro: str
    fecha_desde: str   # DD/MM/AAAA
    fecha_hasta: str   # DD/MM/AAAA


class ErrorLogin(Exception):
    pass

class ErrorDescarga(Exception):
    pass


async def descargar_ambos_excels(params: ParamsDescarga) -> tuple[bytes, bytes]:
    """
    Retorna (bytes_consultas, bytes_prescripciones).
    Lanza ErrorLogin si las credenciales son inválidas.
    Lanza ErrorDescarga ante cualquier otro problema.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            await _login(page, params.usuario, params.password)
            excel_consultas     = await _descargar_consultas(page, params)
            excel_prescripciones = await _descargar_prescripciones(page, params)
            return excel_consultas, excel_prescripciones
        finally:
            await browser.close()


async def _login(page, usuario: str, password: str):
    logger.info("Iniciando login...")
    await page.goto(URL_LOGIN, wait_until="networkidle", timeout=30000)

    try:
        # IDs confirmados del HTML de login (ThinkSoft JSF/PrimeFaces)
        await page.locator("#formLogin\\:j_idt12").fill(usuario, timeout=8000)
        await page.locator("#formLogin\\:j_idt15").fill(password, timeout=5000)

        # El botón usa PrimeFaces.ab() — hay que hacer click, no submit
        await page.locator("#formLogin\\:j_idt17").click(timeout=5000)

        # Esperar redirección a la pantalla de inicio
        await page.wait_for_url("**/inicio.faces**", timeout=20000)
        logger.info("Login exitoso")

    except PWTimeout:
        # Verificar si hay mensaje de error de credenciales en la página
        contenido = await page.content()
        if any(k in contenido.lower() for k in [
            "incorrecto", "invalido", "incorrect", "invalid",
            "no existe", "no encontrado", "error de autenticacion"
        ]):
            raise ErrorLogin("Credenciales incorrectas — verificá usuario y contraseña")
        raise ErrorLogin("No se pudo completar el login. ¿Las credenciales son correctas?")


async def _descargar_consultas(page, params: ParamsDescarga) -> bytes:
    logger.info("Descargando Consultas Atenciones...")
    await page.goto(URL_CONSULTAS, wait_until="networkidle", timeout=20000)

    try:
        await page.wait_for_selector(
            "#formNorth\\:j_idt128_input", timeout=15000
        )
    except PWTimeout:
        raise ErrorDescarga("No se pudo cargar la pantalla de Consultas Atenciones")

    # Fecha Desde — formato DD/MM/YY (el campo acepta 8 chars)
    fecha_desde_corta = _fecha_larga_a_corta(params.fecha_desde)
    fecha_hasta_corta = _fecha_larga_a_corta(params.fecha_hasta)

    await _set_fecha(page, "#formNorth\\:j_idt128_input", fecha_desde_corta)
    await _set_fecha(page, "#formNorth\\:j_idt132_input", fecha_hasta_corta)

    # Centro Atención
    if params.centro:
        await _seleccionar_centro_consultas(page, params.centro)

    # Consultar
    await page.locator("#formNorth\\:j_idt152").click()
    await _esperar_tabla(page, "#formPrincipal\\:j_idt155_data", timeout=45000)

    # Generar Excel
    return await _capturar_descarga(page, "#formSouth\\:j_idt226")


async def _descargar_prescripciones(page, params: ParamsDescarga) -> bytes:
    logger.info("Descargando Prescripciones Emitidas...")
    await page.goto(URL_PRESCRIPCIONES, wait_until="networkidle", timeout=20000)

    # Esperar que cargue el formulario — Prescripciones tiene fecha sin hora
    try:
        # Buscar campo de fecha — puede variar según versión
        campo_desde = page.locator('input[id*="desde"], input[id*="Desde"]').first
        await campo_desde.wait_for(timeout=12000)
    except PWTimeout:
        raise ErrorDescarga("No se pudo cargar la pantalla de Prescripciones Emitidas")

    # Fecha Desde / Hasta (formato DD/MM/AAAA — esta pantalla acepta fecha completa)
    campo_desde = page.locator('input[id*="desde"], input[id*="Desde"]').first
    campo_hasta = page.locator('input[id*="hasta"], input[id*="Hasta"]').first

    await campo_desde.triple_click()
    await campo_desde.fill(params.fecha_desde)
    await campo_desde.press("Tab")
    await page.wait_for_timeout(300)

    await campo_hasta.triple_click()
    await campo_hasta.fill(params.fecha_hasta)
    await campo_hasta.press("Tab")
    await page.wait_for_timeout(300)

    # Centro Atención — en esta pantalla es un dropdown
    if params.centro:
        await _seleccionar_centro_prescripciones(page, params.centro)

    # Consultar
    boton_consultar = page.locator(
        'button:has-text("Consultar"), input[value="Consultar"]'
    ).first
    await boton_consultar.click()
    await _esperar_tabla(page, 'tbody[id*="data"]', timeout=45000)

    # Exportar Excel
    return await _capturar_descarga(
        page,
        'button:has-text("Exportar Excel"), button:has-text("Generar Excel")'
    )


async def _seleccionar_centro_consultas(page, centro: str):
    """Usa el popup buscador de la pantalla de Consultas."""
    try:
        await page.locator("#formNorth\\:j_idt138").click()
        popup = page.locator("#popupBuscadorCentroAtencion")
        await popup.wait_for(state="visible", timeout=8000)

        campo = page.locator("#formBuscadorCentroAtencion\\:j_idt258")
        await campo.fill(centro)
        await page.locator("#formBuscadorCentroAtencion\\:j_idt259").click()
        await page.wait_for_timeout(1200)

        primer_resultado = page.locator(
            "#formBuscadorCentroAtencion\\:tablaCentroAtencion_data tr:first-child td"
        )
        await primer_resultado.click(timeout=6000)
        await page.wait_for_timeout(600)
        logger.info(f"Centro seleccionado: {centro}")
    except PWTimeout:
        logger.warning(f"No se encontró el centro '{centro}' — se consulta sin filtro de centro")
        try:
            await page.locator("#formBuscadorCentroAtencion\\:j_idt268").click()
        except Exception:
            pass


async def _seleccionar_centro_prescripciones(page, centro: str):
    """Selecciona el centro en el dropdown de Prescripciones."""
    try:
        select = page.locator('select[id*="centro"], select[id*="Centro"]').first
        await select.select_option(label=centro, timeout=5000)
    except Exception:
        # Si no es un select estándar, intentar con PrimeFaces selectonemenu
        try:
            combo = page.locator('[id*="centro"] .ui-selectonemenu-label').first
            await combo.click(timeout=4000)
            opcion = page.locator(f'li:has-text("{centro}")').first
            await opcion.click(timeout=4000)
        except Exception:
            logger.warning(f"No se pudo seleccionar centro '{centro}' en Prescripciones — continuando sin filtro")


async def _set_fecha(page, selector: str, fecha_corta: str):
    """Escribe una fecha en un campo datepicker JSF."""
    campo = page.locator(selector)
    await campo.triple_click()
    await campo.fill(fecha_corta)
    await campo.press("Tab")
    await page.wait_for_timeout(200)


async def _esperar_tabla(page, selector: str, timeout: int = 30000):
    """Espera que la tabla tenga datos o confirma que está vacía."""
    try:
        await page.wait_for_selector(selector, state="visible", timeout=timeout)
        await page.wait_for_timeout(500)
    except PWTimeout:
        raise ErrorDescarga(f"Tiempo de espera agotado esperando resultados ({selector})")


async def _capturar_descarga(page, selector: str) -> bytes:
    """Hace clic en un botón de descarga y captura el archivo en memoria."""
    try:
        async with page.expect_download(timeout=60000) as descarga_info:
            await page.locator(selector).first.click()
        descarga = await descarga_info.value
        stream = io.BytesIO()
        await descarga.save_as("/tmp/_tmp_descarga.xlsx")
        with open("/tmp/_tmp_descarga.xlsx", "rb") as f:
            return f.read()
    except PWTimeout:
        raise ErrorDescarga("Tiempo de espera agotado al descargar el archivo Excel")


def _fecha_larga_a_corta(fecha: str) -> str:
    """
    Convierte DD/MM/AAAA → DD/MM/AA (formato de 8 chars que acepta el HIS).
    Ejemplo: 01/03/2026 → 01/03/26
    """
    partes = fecha.strip().split("/")
    if len(partes) == 3 and len(partes[2]) == 4:
        return f"{partes[0]}/{partes[1]}/{partes[2][2:]}"
    return fecha  # ya viene en formato corto
